package oa

import (
	"encoding/json"
	"errors"
	"fmt"

	"github.com/nextlevelbuilder/goclaw/internal/bus"
	"github.com/nextlevelbuilder/goclaw/internal/channels"
	"github.com/nextlevelbuilder/goclaw/internal/config"
	"github.com/nextlevelbuilder/goclaw/internal/store"
)

// Factory returns a channels.ChannelFactory closure that captures the
// store dependency. The store handle is needed by phase 02 to persist
// refreshed tokens. Instance-ID resolution is deferred to phase 02 via
// a setter on Channel — phase 01 doesn't need it (no refresh, no Send).
func Factory(ciStore store.ChannelInstanceStore) channels.ChannelFactory {
	return func(name string, credsRaw json.RawMessage, cfgRaw json.RawMessage,
		msgBus *bus.MessageBus, pairingSvc store.PairingStore) (channels.Channel, error) {

		if ciStore == nil {
			return nil, errors.New("zalo_oa: nil ChannelInstanceStore")
		}

		creds, err := LoadCreds(credsRaw)
		if err != nil {
			return nil, fmt.Errorf("zalo_oa: decode credentials: %w", err)
		}

		var cfg config.ZaloOAConfig
		if len(cfgRaw) > 0 {
			if err := json.Unmarshal(cfgRaw, &cfg); err != nil {
				return nil, fmt.Errorf("zalo_oa: decode config: %w", err)
			}
		}

		ch, err := New(name, cfg, creds, ciStore, msgBus, pairingSvc)
		if err != nil {
			return nil, err
		}
		// Seed the in-memory poll cursor from any persisted state in
		// channel_instances.config.poll_cursor (phase-04 persistence).
		if seeded := parseCursorFromConfig(cfgRaw); len(seeded) > 0 {
			ch.cursor.loadFromMap(seeded)
		}
		return ch, nil
	}
}
