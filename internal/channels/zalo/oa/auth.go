package oa

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"strconv"
	"strings"
	"time"
)

// ErrAuthExpired indicates the refresh token is no longer valid (single-use
// rotation burned, or operator revoked the OA permission). Caller must
// surface this to the operator and block further refreshes until re-auth.
var ErrAuthExpired = errors.New("zalo_oa: refresh token expired, re-auth required")

// ErrNotAuthorized indicates the channel has not yet completed the
// paste-code consent flow (no refresh token persisted). Distinct from
// ErrAuthExpired: this is a "not started" state, not a failure — health
// reporting should stay Degraded (awaiting consent), not Failed.
var ErrNotAuthorized = errors.New("zalo_oa: not yet authorized (paste consent code first)")

// classifyRefreshError maps a refresh-call error to either ErrAuthExpired
// (final, requires operator action) or returns the original error (transient,
// safe to retry on the next ticker cycle).
//
// Match is conservative: only the OAuth-standard "invalid_grant" token or
// the literal "expired" word in the Zalo envelope escalates to ErrAuthExpired.
// Generic words like "invalid app_id" or "invalid parameter" stay transient
// (those would mean operator misconfiguration, not refresh-token death — we
// don't want one bad config push to permanently sideline the channel).
func classifyRefreshError(err error) error {
	if err == nil {
		return nil
	}
	var apiErr *APIError
	if errors.As(err, &apiErr) {
		msg := strings.ToLower(apiErr.Message)
		if strings.Contains(msg, "invalid_grant") || strings.Contains(msg, "expired") {
			return fmt.Errorf("%w (zalo error %d: %s)", ErrAuthExpired, apiErr.Code, apiErr.Message)
		}
	}
	return err
}

// Tokens is the parsed OAuth response.
type Tokens struct {
	AccessToken  string
	RefreshToken string
	ExpiresAt    time.Time
}

// tokenResponse mirrors Zalo's OAuth v4 response body. Unknown fields
// are tolerated (forward-compat). expires_in has been observed as both
// a number AND a quoted string ("3600") depending on the endpoint, so
// we use flexSeconds to accept either.
type tokenResponse struct {
	AccessToken  string      `json:"access_token"`
	RefreshToken string      `json:"refresh_token"`
	ExpiresIn    flexSeconds `json:"expires_in"`
}

// flexSeconds accepts either a JSON number (3600) or a JSON string ("3600").
// Zalo's OA OAuth endpoint returns the latter form in practice, even though
// the ChickenAI SDK types it as a number — belt-and-suspenders.
type flexSeconds int64

func (f *flexSeconds) UnmarshalJSON(b []byte) error {
	s := strings.Trim(string(b), `"`)
	if s == "" || s == "null" {
		return nil
	}
	n, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		return fmt.Errorf("expires_in: %w", err)
	}
	*f = flexSeconds(n)
	return nil
}

// ExchangeCode swaps an authorization code for an (access, refresh) token pair.
// POST oauth.zaloapp.com/v4/oa/access_token, secret_key in HEADER (not body).
func (c *Client) ExchangeCode(ctx context.Context, appID, secretKey, code string) (*Tokens, error) {
	form := url.Values{
		"app_id":     {appID},
		"code":       {code},
		"grant_type": {"authorization_code"},
	}
	return c.tokenCall(ctx, secretKey, form)
}

// RefreshToken trades a refresh token for a new (access, refresh) pair.
// Refresh tokens are SINGLE-USE — every successful refresh rotates both.
func (c *Client) RefreshToken(ctx context.Context, appID, secretKey, refresh string) (*Tokens, error) {
	form := url.Values{
		"app_id":        {appID},
		"refresh_token": {refresh},
		"grant_type":    {"refresh_token"},
	}
	return c.tokenCall(ctx, secretKey, form)
}

func (c *Client) tokenCall(ctx context.Context, secretKey string, form url.Values) (*Tokens, error) {
	headers := map[string]string{"secret_key": secretKey}
	raw, err := c.postForm(ctx, c.oauthBase+pathOAuthAccessToken, headers, form)
	if err != nil {
		return nil, err
	}
	var resp tokenResponse
	if err := json.Unmarshal(raw, &resp); err != nil {
		return nil, fmt.Errorf("decode token response: %w", err)
	}
	if resp.AccessToken == "" {
		return nil, fmt.Errorf("zalo oauth: empty access_token in response")
	}
	exp := time.Now().UTC().Add(time.Duration(resp.ExpiresIn) * time.Second)
	return &Tokens{
		AccessToken:  resp.AccessToken,
		RefreshToken: resp.RefreshToken,
		ExpiresAt:    exp,
	}, nil
}

// ConsentURL builds the redirect URL the operator visits to authorize the OA.
// Returned URL embeds the supplied state token for CSRF protection (validated
// in the WS exchange_code handler).
func ConsentURL(appID, redirectURI, state string) string {
	q := url.Values{
		"app_id":       {appID},
		"redirect_uri": {redirectURI},
		"state":        {state},
	}
	return defaultOAuthBase + "/oa/permission?" + q.Encode()
}
