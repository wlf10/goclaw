package oa

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/url"
	"sort"
	"time"

	"github.com/nextlevelbuilder/goclaw/internal/channels"
)

// message is a single entry in the /v2.0/oa/listrecentchat response. This
// endpoint returns the most-recent N messages across all users — each row
// IS a message, not a thread summary. The live response shape (verified
// against openapi.zalo.me via API explorer, 2026-04-20):
//
//	{"error":0,"message":"Success","data":[{
//	   "from_id":"...", "from_display_name":"...", "from_avatar":"...",
//	   "to_id":"...",   "to_display_name":"...",   "to_avatar":"...",
//	   "message_id":"...", "type":"text", "message":"...", "time":<unix-ms>
//	}]}
//
// Filter: from_id == creds.OAID means OA outbound echo — skip.
// The remaining fields are non-sensitive metadata we pass through as
// bus.InboundMessage.Metadata when useful.
type message struct {
	MessageID       string `json:"message_id"`
	FromID          string `json:"from_id"`
	FromDisplayName string `json:"from_display_name,omitempty"`
	ToID            string `json:"to_id,omitempty"`
	Time            int64  `json:"time,omitempty"`
	Text            string `json:"message,omitempty"` // Zalo's field is "message", not "text"
	Type            string `json:"type,omitempty"`    // text/image/file/sticker
}

// listRecentChat fetches the most-recent N messages across all users.
// Zalo v2.0 encodes GET params as a single JSON blob in the `data` query
// parameter (e.g. ?data={"offset":0,"count":10}).
func (c *Channel) listRecentChat(ctx context.Context, offset, count int) ([]message, error) {
	tok, err := c.tokens.Access(ctx)
	if err != nil {
		return nil, err
	}
	data, err := json.Marshal(map[string]int{"offset": offset, "count": count})
	if err != nil {
		return nil, fmt.Errorf("zalo_oa: marshal listrecentchat params: %w", err)
	}
	q := url.Values{"data": {string(data)}}
	raw, err := c.client.apiGet(ctx, pathListRecentChat, q, tok)
	if err != nil {
		return nil, err
	}
	var wrap struct {
		Data []message `json:"data"`
	}
	if err := json.Unmarshal(raw, &wrap); err != nil {
		return nil, fmt.Errorf("zalo_oa: decode listrecentchat: %w", err)
	}
	return wrap.Data, nil
}

// pollOnce runs one polling cycle. Returns ErrRateLimit if Zalo signals
// 429 (caller should back off); other errors are transient and the next
// cycle retries normally. Retry-once-on-auth mirrors Channel.post so a
// revoked token gets a chance to refresh before we give up.
//
// Design: listrecentchat returns the last N messages across all users
// (NOT a thread summary — each row is a message, verified via API
// explorer 2026-04-20). We iterate oldest-first, filter OA echoes
// (from_id == oa_id), dedup per-user by last-seen timestamp, and
// dispatch via BaseChannel.HandleMessage.
//
// v1 limitation: the listrecentchat window is bounded by `count`
// (default 10). High-volume OAs can have messages rotate off the
// window between polls. Webhook upgrade (v2) is the structural fix.
func (c *Channel) pollOnce(ctx context.Context) error {
	if c.skipPollIfAuthFailed() {
		return nil
	}

	msgs, err := c.listRecentChat(ctx, 0, listRecentChatCount)
	if err != nil {
		var apiErr *APIError
		if errors.As(err, &apiErr) && apiErr.isAuth() {
			slog.Warn("zalo_oa.poll.token_rejected_forcing_refresh",
				"oa_id", c.creds.OAID, "zalo_code", apiErr.Code, "zalo_msg", apiErr.Message)
			c.tokens.ForceRefresh()
			msgs, err = c.listRecentChat(ctx, 0, listRecentChatCount)
		}
		if err != nil {
			return err
		}
	}

	// Process oldest-first so the cursor advances monotonically.
	sort.SliceStable(msgs, func(i, j int) bool { return msgs[i].Time < msgs[j].Time })

	for _, m := range msgs {
		if m.FromID == "" || m.FromID == c.creds.OAID {
			continue // drop malformed + OA echoes
		}
		// Dedup by the (from_id, time) cursor. When time == 0 (Zalo
		// omitted the field) we fall back to message_id dedup via the
		// cursor's dirty flag — a message can still re-emit once if we
		// restart inside the same poll window, which is acceptable.
		if m.Time != 0 && m.Time <= c.cursor.Get(m.FromID) {
			continue
		}
		c.dispatchInbound(m)
		if m.Time != 0 {
			c.cursor.Advance(m.FromID, m.Time)
		}
	}
	return nil
}

// dispatchInbound maps a Zalo message into a BaseChannel.HandleMessage call.
// Zalo OA is DM-only, so chatID == senderID (the user's Zalo ID). Phase 04
// emits text only — non-text payloads are logged and skipped.
func (c *Channel) dispatchInbound(m message) {
	if m.Type != "" && m.Type != "text" {
		slog.Info("zalo_oa.poll.non_text_skipped",
			"oa_id", c.creds.OAID, "user_id", m.FromID, "message_id", m.MessageID, "type", m.Type)
		return
	}
	if m.Text == "" {
		return
	}
	metadata := map[string]string{
		"message_id": m.MessageID,
		"platform":   "zalo_oa",
	}
	if m.FromDisplayName != "" {
		metadata["sender_display_name"] = m.FromDisplayName
	}
	c.BaseChannel.HandleMessage(m.FromID, m.FromID, m.Text, nil, metadata, "direct")
}

// skipPollIfAuthFailed mirrors safety-ticker's skip behavior: once health
// is Failed/Auth, we stop calling the API until the operator re-auths.
func (c *Channel) skipPollIfAuthFailed() bool {
	snap := c.HealthSnapshot()
	return snap.State == channels.ChannelHealthStateFailed && snap.FailureKind == channels.ChannelFailureKindAuth
}

const (
	listRecentChatCount = 10
	defaultPollInterval = 15 * time.Second
	rateLimitBackoff    = 30 * time.Second
	cursorFlushInterval = 60 * time.Second
)

// pollIntervalFromCfg clamps cfg.PollIntervalSeconds to the safe range.
func pollIntervalFromCfg(s int) time.Duration {
	switch {
	case s < 5:
		return defaultPollInterval
	case s > 120:
		return 120 * time.Second
	default:
		return time.Duration(s) * time.Second
	}
}
