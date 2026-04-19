package methods

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/google/uuid"

	"github.com/nextlevelbuilder/goclaw/internal/bus"
	"github.com/nextlevelbuilder/goclaw/internal/channels"
	zalooauth "github.com/nextlevelbuilder/goclaw/internal/channels/zalo/oauth"
	"github.com/nextlevelbuilder/goclaw/internal/gateway"
	"github.com/nextlevelbuilder/goclaw/internal/i18n"
	"github.com/nextlevelbuilder/goclaw/internal/store"
	"github.com/nextlevelbuilder/goclaw/pkg/protocol"
)

const (
	zaloOAuthStateTTL = 10 * time.Minute
	// zaloOAuthDefaultRedirectURI is used only when the instance's creds
	// don't carry one. Zalo enforces redirect_uri match against the
	// dev-console-registered callback (error_code=-14003), so this
	// placeholder is never going to work in practice — operators MUST
	// set creds.redirect_uri to their registered callback.
	zaloOAuthDefaultRedirectURI = "https://oa.local/zalo_oauth_callback"
)

// ZaloOAuthMethods serves the WS handlers backing the paste-code consent flow.
type ZaloOAuthMethods struct {
	store  store.ChannelInstanceStore
	msgBus *bus.MessageBus

	stateMu sync.Mutex
	states  map[string]zaloOAuthStateEntry // key: instanceID|state
}

type zaloOAuthStateEntry struct {
	expiresAt time.Time
}

// NewZaloOAuthMethods constructs the handler. msgBus may be nil during tests.
func NewZaloOAuthMethods(s store.ChannelInstanceStore, msgBus *bus.MessageBus) *ZaloOAuthMethods {
	return &ZaloOAuthMethods{
		store:  s,
		msgBus: msgBus,
		states: make(map[string]zaloOAuthStateEntry),
	}
}

// Register wires the methods into the WS router.
func (m *ZaloOAuthMethods) Register(router *gateway.MethodRouter) {
	router.Register(protocol.MethodChannelInstancesZaloOAuthConsentURL, m.handleConsentURL)
	router.Register(protocol.MethodChannelInstancesZaloOAuthExchangeCode, m.handleExchangeCode)
}

// handleConsentURL builds the Zalo authorization URL server-side so the
// frontend never receives app_id (which is masked in maskInstance anyway).
func (m *ZaloOAuthMethods) handleConsentURL(ctx context.Context, client *gateway.Client, req *protocol.RequestFrame) {
	locale := store.LocaleFromContext(ctx)
	var params struct {
		InstanceID string `json:"instance_id"`
	}
	if req.Params != nil {
		_ = json.Unmarshal(req.Params, &params)
	}
	instID, err := uuid.Parse(params.InstanceID)
	if err != nil {
		client.SendResponse(protocol.NewErrorResponse(req.ID, protocol.ErrInvalidRequest, i18n.T(locale, i18n.MsgInvalidID, "instance")))
		return
	}

	inst, err := m.store.Get(ctx, instID)
	if err != nil {
		client.SendResponse(protocol.NewErrorResponse(req.ID, protocol.ErrNotFound, i18n.T(locale, i18n.MsgInstanceNotFound)))
		return
	}
	if inst.ChannelType != channels.TypeZaloOAuth {
		client.SendResponse(protocol.NewErrorResponse(req.ID, protocol.ErrInvalidRequest, i18n.T(locale, i18n.MsgZaloOAuthInvalidChannelType)))
		return
	}

	creds, err := zalooauth.LoadCreds(inst.Credentials)
	if err != nil || creds.AppID == "" {
		client.SendResponse(protocol.NewErrorResponse(req.ID, protocol.ErrInternal, "zalo_oauth: missing app_id in credentials"))
		return
	}

	state, err := newStateToken()
	if err != nil {
		client.SendResponse(protocol.NewErrorResponse(req.ID, protocol.ErrInternal, "zalo_oauth: state token gen failed"))
		return
	}
	m.putState(instID, state)

	redirectURI := creds.RedirectURI
	if redirectURI == "" {
		redirectURI = zaloOAuthDefaultRedirectURI
	}
	url := zalooauth.ConsentURL(creds.AppID, redirectURI, state)
	client.SendResponse(protocol.NewOKResponse(req.ID, map[string]any{
		"url":   url,
		"state": state,
	}))
}

// handleExchangeCode swaps the pasted authorization code for tokens and
// persists them via the store-encrypted credentials blob.
func (m *ZaloOAuthMethods) handleExchangeCode(ctx context.Context, client *gateway.Client, req *protocol.RequestFrame) {
	locale := store.LocaleFromContext(ctx)
	var params struct {
		InstanceID string `json:"instance_id"`
		Code       string `json:"code"`
		State      string `json:"state"`
		OAID       string `json:"oa_id"` // optional — from the callback URL query string
	}
	if req.Params != nil {
		_ = json.Unmarshal(req.Params, &params)
	}
	instID, err := uuid.Parse(params.InstanceID)
	if err != nil {
		client.SendResponse(protocol.NewErrorResponse(req.ID, protocol.ErrInvalidRequest, i18n.T(locale, i18n.MsgInvalidID, "instance")))
		return
	}
	if params.Code == "" {
		client.SendResponse(protocol.NewErrorResponse(req.ID, protocol.ErrInvalidRequest, i18n.T(locale, i18n.MsgRequired, "code")))
		return
	}
	if !m.consumeState(instID, params.State) {
		client.SendResponse(protocol.NewErrorResponse(req.ID, protocol.ErrInvalidRequest, i18n.T(locale, i18n.MsgZaloOAuthInvalidState)))
		return
	}

	inst, err := m.store.Get(ctx, instID)
	if err != nil {
		client.SendResponse(protocol.NewErrorResponse(req.ID, protocol.ErrNotFound, i18n.T(locale, i18n.MsgInstanceNotFound)))
		return
	}
	if inst.ChannelType != channels.TypeZaloOAuth {
		client.SendResponse(protocol.NewErrorResponse(req.ID, protocol.ErrInvalidRequest, i18n.T(locale, i18n.MsgZaloOAuthInvalidChannelType)))
		return
	}

	creds, err := zalooauth.LoadCreds(inst.Credentials)
	if err != nil {
		client.SendResponse(protocol.NewErrorResponse(req.ID, protocol.ErrInternal, i18n.T(locale, i18n.MsgZaloOAuthCodeExchangeFailed, err.Error())))
		return
	}

	httpClient := zalooauth.NewClient(15 * time.Second)
	tok, err := httpClient.ExchangeCode(ctx, creds.AppID, creds.SecretKey, params.Code)
	if err != nil {
		slog.Warn("zalo_oauth.exchange_failed", "instance_id", instID, "oa_id", creds.OAID, "error", err)
		client.SendResponse(protocol.NewErrorResponse(req.ID, protocol.ErrInternal, i18n.T(locale, i18n.MsgZaloOAuthCodeExchangeFailed, err.Error())))
		return
	}
	creds.WithTokens(tok)
	// Zalo's OAuth token endpoint does NOT return oa_id; it rides in the
	// callback URL query string alongside `code`. Persist it here so the
	// reloaded Channel's Start() sees a non-empty OAID and marks Healthy
	// (otherwise it stays Degraded "awaiting consent" forever).
	if params.OAID != "" {
		creds.OAID = params.OAID
	}
	credsBytes, err := creds.Marshal()
	if err != nil {
		client.SendResponse(protocol.NewErrorResponse(req.ID, protocol.ErrInternal, i18n.T(locale, i18n.MsgZaloOAuthCodeExchangeFailed, err.Error())))
		return
	}
	if err := m.store.Update(ctx, instID, map[string]any{"credentials": credsBytes}); err != nil {
		client.SendResponse(protocol.NewErrorResponse(req.ID, protocol.ErrInternal, i18n.T(locale, i18n.MsgZaloOAuthCodeExchangeFailed, err.Error())))
		return
	}
	m.emitCacheInvalidate()

	slog.Info("zalo_oauth.connected", "instance_id", instID, "oa_id", creds.OAID, "expires_at", tok.ExpiresAt)
	client.SendResponse(protocol.NewOKResponse(req.ID, map[string]any{
		"ok":         true,
		"oa_id":      creds.OAID,
		"expires_at": tok.ExpiresAt,
	}))
}

func (m *ZaloOAuthMethods) emitCacheInvalidate() {
	if m.msgBus == nil {
		return
	}
	m.msgBus.Broadcast(bus.Event{
		Name:    protocol.EventCacheInvalidate,
		Payload: bus.CacheInvalidatePayload{Kind: bus.CacheKindChannelInstances},
	})
}

// putState records a freshly minted state token with a 10min TTL.
func (m *ZaloOAuthMethods) putState(instID uuid.UUID, state string) {
	m.stateMu.Lock()
	defer m.stateMu.Unlock()
	m.gcStatesLocked()
	m.states[stateKey(instID, state)] = zaloOAuthStateEntry{expiresAt: time.Now().Add(zaloOAuthStateTTL)}
}

// consumeState atomically validates+removes a state token. Returns false
// if missing or expired.
func (m *ZaloOAuthMethods) consumeState(instID uuid.UUID, state string) bool {
	if state == "" {
		return false
	}
	m.stateMu.Lock()
	defer m.stateMu.Unlock()
	key := stateKey(instID, state)
	entry, ok := m.states[key]
	if !ok || time.Now().After(entry.expiresAt) {
		delete(m.states, key) // GC the expired entry too
		return false
	}
	delete(m.states, key)
	return true
}

func (m *ZaloOAuthMethods) gcStatesLocked() {
	now := time.Now()
	for k, v := range m.states {
		if now.After(v.expiresAt) {
			delete(m.states, k)
		}
	}
}

func stateKey(instID uuid.UUID, state string) string {
	return fmt.Sprintf("%s|%s", instID, state)
}

func newStateToken() (string, error) {
	b := make([]byte, 32)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return hex.EncodeToString(b), nil
}
