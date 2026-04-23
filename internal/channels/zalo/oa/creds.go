// Package zalooauth implements the phone-number-tied Zalo Official Account
// channel using OAuth v4 (oauth.zaloapp.com + openapi.zalo.me). Distinct
// from internal/channels/zalo (Bot OA, static token) and zalo/personal
// (QR personal). Different auth, different host, different message shapes.
package oa

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"

	"github.com/nextlevelbuilder/goclaw/internal/store"
)

// ChannelCreds is the plaintext shape of the credentials JSON stored
// inside the channel_instances.credentials BLOB. The store layer encrypts
// the entire blob — do NOT call crypto.Encrypt/Decrypt on individual fields.
type ChannelCreds struct {
	AppID     string `json:"app_id"`
	SecretKey string `json:"secret_key"`
	OAID      string `json:"oa_id,omitempty"`

	// RedirectURI must match the callback URL registered on the Zalo dev
	// console. Zalo returns error_code=-14003 "Invalid redirect uri" if
	// these don't match. Operator-set per instance — pick any URL you have
	// registered (a static "copy the code" page works fine).
	RedirectURI string `json:"redirect_uri,omitempty"`

	AccessToken   string    `json:"access_token,omitempty"`
	RefreshToken  string    `json:"refresh_token,omitempty"`
	ExpiresAt     time.Time `json:"expires_at,omitempty"`
	LastRefreshAt time.Time `json:"last_refresh_at,omitempty"`
}

// LoadCreds parses plaintext credential JSON. The store layer has already
// decrypted the surrounding blob.
func LoadCreds(raw json.RawMessage) (*ChannelCreds, error) {
	var c ChannelCreds
	if err := json.Unmarshal(raw, &c); err != nil {
		return nil, err
	}
	return &c, nil
}

// Marshal returns plaintext JSON. The store layer re-encrypts on Update.
func (c *ChannelCreds) Marshal() (json.RawMessage, error) {
	return json.Marshal(c)
}

// WithTokens copies new tokens onto the receiver and stamps LastRefreshAt.
// Caller must pass a non-nil tok — passing nil indicates a programming error
// upstream (refresh/exchange should never return (nil, nil)).
func (c *ChannelCreds) WithTokens(tok *Tokens) {
	c.AccessToken = tok.AccessToken
	c.RefreshToken = tok.RefreshToken
	c.ExpiresAt = tok.ExpiresAt
	c.LastRefreshAt = time.Now().UTC()
}

// Persist marshals the (plaintext) creds and writes the resulting blob to
// the channel_instances row. The store layer re-encrypts on Update, so this
// function does NO field-level encryption.
func Persist(ctx context.Context, s store.ChannelInstanceStore, id uuid.UUID, c *ChannelCreds) error {
	if s == nil {
		return fmt.Errorf("zalo_oa: nil ChannelInstanceStore in Persist")
	}
	if id == uuid.Nil {
		return fmt.Errorf("zalo_oa: nil instance ID in Persist")
	}
	blob, err := c.Marshal()
	if err != nil {
		return fmt.Errorf("zalo_oa: marshal creds: %w", err)
	}
	return s.Update(ctx, id, map[string]any{"credentials": []byte(blob)})
}
