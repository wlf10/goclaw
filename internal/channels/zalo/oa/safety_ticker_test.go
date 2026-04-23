package oa

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/nextlevelbuilder/goclaw/internal/bus"
	"github.com/nextlevelbuilder/goclaw/internal/config"
)

// TestStartStop_TickerShutsDownPromptly proves the safety-ticker goroutine
// exits within a bounded time when Stop() is called. Failure mode being
// guarded: a leaked goroutine keeps polling forever after channel removal.
func TestStartStop_TickerShutsDownPromptly(t *testing.T) {
	t.Parallel()

	cfg := config.ZaloOAConfig{
		AppID:               "app",
		SecretKey:           "key",
		SafetyTickerMinutes: 1, // value irrelevant — we Stop before any tick fires
	}
	creds := &ChannelCreds{
		AppID:        "app",
		SecretKey:    "key",
		AccessToken:  "AT",
		RefreshToken: "RT",
		ExpiresAt:    time.Now().Add(time.Hour),
	}
	msgBus := bus.New()

	c, err := New("test_inst", cfg, creds, &fakeStore{}, msgBus, nil)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	c.SetInstanceID(uuid.New())

	if err := c.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}

	done := make(chan struct{})
	go func() {
		_ = c.Stop(context.Background())
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Stop did not return within 2s — ticker goroutine leaked")
	}
}

// TestSafetyTicker_RefreshesWhenWithinThreshold verifies the ticker calls
// Access() (which triggers refresh) when the token sits inside the safety
// threshold. We don't measure timing precisely — just that within a few
// short ticks the upstream gets called.
func TestSafetyTicker_RefreshesWhenWithinThreshold(t *testing.T) {
	t.Parallel()

	srv, count := newRefreshServer(t, "")
	fs := &fakeStore{}

	cfg := config.ZaloOAConfig{
		AppID:     "app",
		SecretKey: "key",
		// 1-second ticker so the test runs quickly. Forced via newWithInterval helper.
	}
	creds := &ChannelCreds{
		AppID:        "app",
		SecretKey:    "key",
		AccessToken:  "AT-old",
		RefreshToken: "RT-old",
		ExpiresAt:    time.Now().Add(30 * time.Second), // well inside the safety threshold
	}
	msgBus := bus.New()

	c, err := New("test_inst", cfg, creds, fs, msgBus, nil)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	c.SetInstanceID(uuid.New())
	// Override the upstream OAuth host for the test.
	c.tokens.client.oauthBase = srv.URL
	// Override the ticker interval so the test doesn't wait the production default.
	c.safetyTickerInterval = 100 * time.Millisecond

	if err := c.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer func() { _ = c.Stop(context.Background()) }()

	// Wait up to 2s for the ticker to fire and trigger one refresh.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if atomic.LoadInt32(count) >= 1 && fs.UpdateCount() >= 1 {
			return // pass
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("ticker did not refresh within 2s: refresh=%d, updates=%d", atomic.LoadInt32(count), fs.UpdateCount())
}
