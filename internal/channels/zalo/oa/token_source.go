package oa

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"time"

	"github.com/google/uuid"

	"github.com/nextlevelbuilder/goclaw/internal/store"
)

// refreshMargin matches internal/oauth/token.go:33 — refresh when the access
// token expires within this window.
const refreshMargin = 5 * time.Minute

// tokenSource is a lazy refresher for the channel's access token. It mirrors
// internal/oauth/token.go DBTokenSource: a single mutex guards both the cache
// and the HTTP refresh, so concurrent callers serialize naturally and only
// one refresh ever flies (Zalo refresh tokens are single-use — racing
// goroutines would invalidate each other's tokens).
type tokenSource struct {
	client     *Client
	creds      *ChannelCreds
	store      store.ChannelInstanceStore
	instanceID uuid.UUID

	mu sync.Mutex // guards creds.{Access,Refresh}Token + ExpiresAt + serializes refresh
}

// ForceRefresh marks the cached token as stale so the NEXT Access() call
// performs an HTTP refresh. Used by Send when the API returns an auth-class
// error mid-call (token rotated externally or a clock skew issue).
func (ts *tokenSource) ForceRefresh() {
	ts.mu.Lock()
	defer ts.mu.Unlock()
	ts.creds.ExpiresAt = time.Time{} // zero → time.Until == negative → triggers refresh
}

// Access returns a currently-valid access token, refreshing under the same
// mutex if the cached token is within `refreshMargin` of expiry.
func (ts *tokenSource) Access(ctx context.Context) (string, error) {
	ts.mu.Lock()
	defer ts.mu.Unlock()

	if ts.creds.AccessToken != "" && time.Until(ts.creds.ExpiresAt) > refreshMargin {
		return ts.creds.AccessToken, nil
	}

	if err := ts.doRefresh(ctx); err != nil {
		return "", err
	}
	return ts.creds.AccessToken, nil
}

// doRefresh performs the HTTP refresh + persistence. Caller MUST hold ts.mu.
//
// Ordering: persist-before-commit. We snapshot a copy of creds with the new
// tokens, persist that snapshot, and only swap the live creds on success.
// Rationale: Zalo refresh tokens are single-use, so the upstream call ALREADY
// burned the old refresh token. If Persist fails, the live creds in memory
// stay on the new tokens (because we still need them to keep working until
// process restart) BUT the DB has the stale tokens. On restart, the next
// refresh attempt with the stale refresh token returns invalid_grant →
// ErrAuthExpired → operator re-auth. This is the best safe failure mode.
func (ts *tokenSource) doRefresh(ctx context.Context) error {
	if ts.creds.RefreshToken == "" {
		// Distinct sentinel: pre-authorization (paste-code not yet exchanged)
		// is NOT the same as a burned refresh token. Caller's
		// markAuthFailedIfNeeded should NOT escalate this to Failed.
		return ErrNotAuthorized
	}

	tok, rawErr := ts.client.RefreshToken(ctx, ts.creds.AppID, ts.creds.SecretKey, ts.creds.RefreshToken)
	if rawErr != nil {
		err := classifyRefreshError(rawErr)
		if errors.Is(err, ErrAuthExpired) {
			slog.Warn("zalo_oa.reauth_required", "instance_id", ts.instanceID, "oa_id", ts.creds.OAID)
			return err
		}
		slog.Warn("zalo_oa.refresh_failed", "instance_id", ts.instanceID, "oa_id", ts.creds.OAID, "error", err)
		return err
	}

	// Build a snapshot copy of creds with the new tokens, persist, then commit.
	snapshot := *ts.creds
	snapshot.WithTokens(tok)
	if err := Persist(ctx, ts.store, ts.instanceID, &snapshot); err != nil {
		slog.Error("zalo_oa.persist_failed", "instance_id", ts.instanceID, "oa_id", ts.creds.OAID, "error", err)
		// Commit to memory anyway: the burned refresh token is the only one
		// we have; the new pair must remain usable until process restart.
		*ts.creds = snapshot
		return err
	}
	*ts.creds = snapshot
	slog.Info("zalo_oa.token_refreshed", "instance_id", ts.instanceID, "oa_id", ts.creds.OAID, "new_expires_at", ts.creds.ExpiresAt)
	return nil
}
