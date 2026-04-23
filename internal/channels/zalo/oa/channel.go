package oa

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"mime"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"

	"github.com/nextlevelbuilder/goclaw/internal/bus"
	"github.com/nextlevelbuilder/goclaw/internal/channels"
	"github.com/nextlevelbuilder/goclaw/internal/config"
	"github.com/nextlevelbuilder/goclaw/internal/store"
)

// ErrPartialSend signals that an attachment was delivered but the trailing
// caption/text message failed. The attachment-side message_id is logged
// alongside the warning; callers may use errors.Is to special-case retry.
var ErrPartialSend = errors.New("zalo_oa: attachment delivered but trailing text failed")

const (
	defaultClientTimeout        = 15 * time.Second
	defaultSafetyTickerInterval = 30 * time.Minute
)
// Per-endpoint upload caps (Zalo OA): image 1MB, file 5MB, gif 5MB.
// These are hard-enforced by Zalo's own endpoints (error -210). Defined
// inline at the single callsite in (*Channel).dispatch — see channel.go
// around the dispatch branch.

// Channel is the phase-02 form. Phase 03 wires Send; phase 04 wires polling.
type Channel struct {
	*channels.BaseChannel

	client  *Client
	creds   *ChannelCreds
	ciStore store.ChannelInstanceStore
	cfg     config.ZaloOAConfig

	// instanceID is injected by InstanceLoader via SetInstanceID after construction
	// (ChannelFactory signature doesn't expose it).
	instanceID uuid.UUID

	tokens *tokenSource

	// Polling state (phase 04).
	cursor       *pollCursor
	pollInterval time.Duration
	pollWG       sync.WaitGroup

	// safetyTickerInterval is exposed for tests; production uses defaultSafetyTickerInterval
	// or cfg.SafetyTickerMinutes.
	safetyTickerInterval time.Duration

	stopOnce sync.Once
	stopCh   chan struct{}
	tickerWG sync.WaitGroup
}

// New constructs the channel. InstanceLoader calls SetInstanceID after this.
func New(name string, cfg config.ZaloOAConfig, creds *ChannelCreds,
	ciStore store.ChannelInstanceStore, msgBus *bus.MessageBus, _ store.PairingStore) (*Channel, error) {

	if creds == nil {
		return nil, errors.New("zalo_oa: nil creds")
	}
	if creds.AppID == "" || creds.SecretKey == "" {
		return nil, errors.New("zalo_oa: app_id and secret_key are required")
	}

	c := &Channel{
		BaseChannel:          channels.NewBaseChannel(name, msgBus, []string(cfg.AllowFrom)),
		client:               NewClient(defaultClientTimeout),
		creds:                creds,
		ciStore:              ciStore,
		cfg:                  cfg,
		cursor:               newPollCursor(defaultCursorMaxEntries),
		pollInterval:         pollIntervalFromCfg(cfg.PollIntervalSeconds),
		safetyTickerInterval: tickerInterval(cfg.SafetyTickerMinutes),
		stopCh:               make(chan struct{}),
	}
	c.tokens = &tokenSource{
		client: c.client,
		creds:  c.creds,
		store:  c.ciStore,
	}
	return c, nil
}

// SetInstanceID is called by InstanceLoader after construction. The instance
// ID is needed by the token-refresh path to write back rotated credentials.
func (c *Channel) SetInstanceID(id uuid.UUID) {
	c.instanceID = id
	c.tokens.instanceID = id
}

// SetTestEndpointsForTest overrides the OAuth + API hosts. ONLY for use by
// integration tests that drive the channel against an httptest server.
// Production code paths construct the Client with default endpoints.
func (c *Channel) SetTestEndpointsForTest(oauthBase, apiBase string) {
	if oauthBase != "" {
		c.client.oauthBase = oauthBase
	}
	if apiBase != "" {
		c.client.apiBase = apiBase
	}
}

// ForceRefreshForTest exposes tokenSource.ForceRefresh for integration tests
// that need to bypass the in-memory cache and hit the upstream refresh path.
func (c *Channel) ForceRefreshForTest() {
	c.tokens.ForceRefresh()
}

// Type returns the channel type identifier.
func (c *Channel) Type() string { return channels.TypeZaloOA }

// Start brings the channel up and spawns the safety-ticker goroutine.
// Phase 04 will start the polling loop here.
func (c *Channel) Start(_ context.Context) error {
	c.SetRunning(true)
	if c.creds.OAID != "" {
		slog.Info("zalo_oa.started", "state", "connected", "oa_id", c.creds.OAID, "name", c.Name())
		c.MarkHealthy("connected")
	} else {
		slog.Info("zalo_oa.started", "state", "unauthorized", "name", c.Name())
		c.MarkDegraded("awaiting consent", "no oa_id yet — paste consent code to authorize",
			channels.ChannelFailureKindAuth, true)
	}

	c.tickerWG.Add(1)
	go c.runSafetyTicker()
	c.pollWG.Add(1)
	// Use Background so the loop survives the caller's ctx cancel; Stop()
	// is the canonical exit signal. The loop wraps each cycle in a per-tick
	// ctx so individual API calls still honor a timeout.
	go c.runPollLoop(context.Background())
	return nil
}

// Stop signals both ticker + poll loop to exit and waits for them.
// Best-effort cursor flush happens inside runPollLoop's exit path.
// Idempotent.
func (c *Channel) Stop(_ context.Context) error {
	c.stopOnce.Do(func() { close(c.stopCh) })
	c.tickerWG.Wait()
	c.pollWG.Wait()
	c.SetRunning(false)
	slog.Info("zalo_oa.stopped", "name", c.Name())
	return nil
}

// Send dispatches an outbound message to text / image / file based on the
// Media slice. Phase 03 supports one media element per message; additional
// media in the slice are logged-and-skipped (Zalo OA sends one attachment
// per message). The Media URL is treated as a local file path.
//
// Caption + Content alongside an attachment ride as a SEPARATE text message
// (Zalo OA's attachment payload has no caption field). If that trailing
// text fails after the attachment succeeded, returns ErrPartialSend so
// callers can distinguish from a full failure.
func (c *Channel) Send(ctx context.Context, msg bus.OutboundMessage) error {
	if msg.ChatID == "" {
		return errors.New("zalo_oa: empty user_id")
	}

	if len(msg.Media) == 0 {
		_, err := c.SendText(ctx, msg.ChatID, msg.Content)
		return err
	}
	if len(msg.Media) > 1 {
		slog.Info("zalo_oa.send.extra_media_skipped",
			"oa_id", c.creds.OAID, "extra", len(msg.Media)-1)
	}

	m := msg.Media[0]
	// Generous stat-first guard (50MB) prevents OOM on pathological paths.
	// Per-type caps are enforced below: image auto-compresses to 1MB,
	// file rejects if MIME isn't PDF/DOC/DOCX or >5MB.
	data, mt, err := c.readMedia(m, 50*1024*1024)
	if err != nil {
		return err
	}

	var attachMID string
	if mt == "image/gif" {
		// Zalo has a dedicated /upload/gif endpoint (cap 5MB) that
		// preserves animation. Don't re-encode GIFs as JPEG.
		const zaloGIFCapBytes = 5 * 1024 * 1024
		if len(data) > zaloGIFCapBytes {
			return fmt.Errorf("zalo_oa: gif too large: %d bytes (Zalo cap is 5MB)", len(data))
		}
		attachMID, err = c.SendGIF(ctx, msg.ChatID, data)
	} else if strings.HasPrefix(mt, "image/") {
		// Zalo upload/image caps at 1MB and only accepts jpg/png.
		// Auto-compress oversized or non-jpg/png images to JPEG.
		const zaloImageCapBytes = 1 * 1024 * 1024
		compressed, newMT, cerr := compressForZaloImage(data, mt, zaloImageCapBytes)
		if cerr != nil {
			return cerr
		}
		data, mt = compressed, newMT
		attachMID, err = c.SendImage(ctx, msg.ChatID, data, mt)
	} else {
		// Zalo upload/file only accepts PDF/DOC/DOCX up to 5MB.
		const zaloFileCapBytes = 5 * 1024 * 1024
		if !isZaloSupportedFileMIME(mt) {
			return fmt.Errorf("zalo_oa: file MIME %q not supported (Zalo accepts PDF, DOC, DOCX only)", mt)
		}
		if len(data) > zaloFileCapBytes {
			return fmt.Errorf("zalo_oa: file too large: %d bytes (Zalo cap is 5MB)", len(data))
		}
		attachMID, err = c.SendFile(ctx, msg.ChatID, data, filepath.Base(m.URL))
	}
	if err != nil {
		return err
	}

	trailing := mergeTrailingText(m.Caption, msg.Content)
	if trailing == "" {
		return nil
	}
	if _, terr := c.SendText(ctx, msg.ChatID, trailing); terr != nil {
		slog.Error("zalo_oa.send.text_after_attachment_failed",
			"oa_id", c.creds.OAID, "user_id", msg.ChatID,
			"attachment_message_id", attachMID, "error", terr)
		return fmt.Errorf("%w: %v", ErrPartialSend, terr)
	}
	return nil
}

// mergeTrailingText joins caption + content for the post-attachment text
// message. Each is trimmed; empties are skipped; both present are joined
// with a blank line so the caption stands as its own paragraph.
func mergeTrailingText(caption, content string) string {
	caption = strings.TrimSpace(caption)
	content = strings.TrimSpace(content)
	switch {
	case caption == "" && content == "":
		return ""
	case caption == "":
		return content
	case content == "":
		return caption
	default:
		return caption + "\n\n" + content
	}
}

// readMedia stat-checks the file BEFORE allocating, then reads bytes. The
// stat-first pattern (mirrors telegram/send.go:399) prevents a 2GB malicious
// path from OOMing the process before the size guard rejects it.
func (c *Channel) readMedia(m bus.MediaAttachment, maxBytes int64) ([]byte, string, error) {
	if m.URL == "" {
		return nil, "", errors.New("zalo_oa: media URL empty")
	}
	if maxBytes > 0 {
		info, statErr := os.Stat(m.URL)
		if statErr == nil && info.Size() > maxBytes {
			return nil, "", fmt.Errorf("zalo_oa: media too large: %d bytes (local cap %d; Zalo OA hard-caps uploads at 1MB via error -210)", info.Size(), maxBytes)
		}
	}
	data, err := os.ReadFile(m.URL)
	if err != nil {
		return nil, "", fmt.Errorf("zalo_oa: read media %s: %w", m.URL, err)
	}
	mt := m.ContentType
	if mt == "" {
		mt = mime.TypeByExtension(strings.ToLower(filepath.Ext(m.URL)))
		if mt == "" {
			mt = "application/octet-stream"
		}
	}
	return data, mt, nil
}

// runSafetyTicker calls Access() periodically so idle channels don't let
// the refresh-token rotation window lapse silently. Skips work if the
// channel is already in auth-failed state to avoid log spam.
func (c *Channel) runSafetyTicker() {
	defer c.tickerWG.Done()

	t := time.NewTicker(c.safetyTickerInterval)
	defer t.Stop()

	for {
		select {
		case <-c.stopCh:
			return
		case <-t.C:
			if c.skipTickIfAuthFailed() {
				continue
			}
			// Access() does its own under-mutex check for refreshMargin —
			// we deliberately don't pre-read creds.ExpiresAt here to avoid
			// racing with concurrent refresh writes from Send (phase 03+).
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			if _, err := c.tokens.Access(ctx); err != nil && !errors.Is(err, ErrNotAuthorized) {
				c.markAuthFailedIfNeeded(err)
				slog.Warn("zalo_oa.safety_tick_refresh_failed", "instance_id", c.instanceID, "error", err)
			}
			cancel()
		}
	}
}

// skipTickIfAuthFailed avoids re-attempting refresh once the channel is in
// permanent auth-failed state (operator must re-auth).
func (c *Channel) skipTickIfAuthFailed() bool {
	snap := c.HealthSnapshot()
	return snap.State == channels.ChannelHealthStateFailed && snap.FailureKind == channels.ChannelFailureKindAuth
}

// markAuthFailedIfNeeded transitions health to Failed/Auth on any auth-
// class error. Two shapes qualify:
//
//   - ErrAuthExpired: raised by the tokenSource refresh path when Zalo
//     rejects the refresh token itself (refresh-token dead).
//   - *APIError where isAuth() is true: raised by the poll path when
//     a listrecentchat call 401/-216s AFTER the retry-once-on-auth
//     ForceRefresh attempt. At that point the refresh token is likely
//     still valid but the OA-app association is broken and the operator
//     must re-consent.
//
// ErrNotAuthorized (pre-consent stub state) is intentionally NOT
// escalated — the safety ticker already skips that case.
func (c *Channel) markAuthFailedIfNeeded(err error) {
	if err == nil {
		return
	}
	if errors.Is(err, ErrAuthExpired) {
		c.MarkFailed("Re-auth required",
			"Zalo refresh token expired or invalid; operator must re-paste consent code",
			channels.ChannelFailureKindAuth,
			false,
		)
		return
	}
	var apiErr *APIError
	if errors.As(err, &apiErr) && apiErr.isAuth() {
		c.MarkFailed("Re-auth required",
			fmt.Sprintf("Zalo API rejected access_token after refresh retry (code %d: %s)", apiErr.Code, apiErr.Message),
			channels.ChannelFailureKindAuth,
			false,
		)
	}
}

// tickerInterval clamps the ticker to a sane range.
func tickerInterval(cfgMinutes int) time.Duration {
	switch {
	case cfgMinutes < 5:
		return defaultSafetyTickerInterval
	case cfgMinutes > 120:
		return 120 * time.Minute
	default:
		return time.Duration(cfgMinutes) * time.Minute
	}
}
