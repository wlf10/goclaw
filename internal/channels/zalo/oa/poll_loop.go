package oa

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/nextlevelbuilder/goclaw/internal/channels"
	"github.com/nextlevelbuilder/goclaw/internal/store"
)

// runPollLoop is started by Start() and exits when stopCh closes. It
// runs a polling cycle on each tick; on ErrRateLimit it switches to the
// rate-limit ticker until a clean cycle returns. Cursor flushes are
// debounced (60s by default) so we don't pummel the DB per-message.
func (c *Channel) runPollLoop(parentCtx context.Context) {
	defer c.pollWG.Done()

	t := time.NewTicker(c.pollInterval)
	defer t.Stop()
	flush := time.NewTicker(cursorFlushInterval)
	defer flush.Stop()

	rateLimited := false
	pollCtx := store.WithTenantID(parentCtx, c.TenantID())

	for {
		select {
		case <-c.stopCh:
			c.flushCursorOnExit(pollCtx)
			return
		case <-flush.C:
			if c.cursor.IsDirty() {
				if err := c.flushCursor(pollCtx); err != nil {
					slog.Warn("zalo_oa.poll.cursor_flush_failed", "error", err)
				}
			}
		case <-t.C:
			// Cycle ctx must outlive the underlying HTTP client timeout
			// (30s) — otherwise the ctx fires first and the error says
			// "context deadline exceeded" instead of the real cause.
			cycleCtx, cancel := context.WithTimeout(pollCtx, 45*time.Second)
			err := c.pollOnce(cycleCtx)
			cancel()
			switch {
			case errors.Is(err, ErrRateLimit):
				if !rateLimited {
					c.MarkDegraded("rate limited", err.Error(), channels.ChannelFailureKindNetwork, true)
					t.Reset(rateLimitBackoff)
					rateLimited = true
				}
			case err != nil:
				slog.Warn("zalo_oa.poll_failed", "oa_id", c.creds.OAID, "error", err)
				// Auth-class errors that survive the in-pollOnce retry-
				// once-on-auth mean the operator must re-consent. Flip
				// health so the dashboard surfaces the red re-auth prompt
				// instead of staying green while logs scream.
				c.markAuthFailedIfNeeded(err)
			default:
				if rateLimited {
					c.MarkHealthy("polling")
					t.Reset(c.pollInterval)
					rateLimited = false
				}
			}
		}
	}
}

// flushCursor performs a read-modify-write of the channel_instances.config
// blob, persisting the cursor under the `poll_cursor` key without clobbering
// any operator-set fields.
func (c *Channel) flushCursor(ctx context.Context) error {
	if c.ciStore == nil || c.instanceID == [16]byte{} {
		return errors.New("zalo_oa: cursor flush without store/instance ID")
	}
	inst, err := c.ciStore.Get(ctx, c.instanceID)
	if err != nil {
		return fmt.Errorf("read instance for cursor flush: %w", err)
	}
	return c.persistCursor(ctx, inst.Config)
}

// persistCursor writes the merged config blob. Exposed for tests so the
// merge logic can be exercised without a store.Get round-trip.
func (c *Channel) persistCursor(ctx context.Context, currentConfig []byte) error {
	merged, err := mergeCursorIntoConfig(currentConfig, c.cursor.Snapshot())
	if err != nil {
		return fmt.Errorf("merge cursor into config: %w", err)
	}
	if err := c.ciStore.Update(ctx, c.instanceID, map[string]any{"config": merged}); err != nil {
		return fmt.Errorf("update instance config: %w", err)
	}
	c.cursor.ClearDirty()
	return nil
}

// flushCursorOnExit is best-effort cursor persistence at Stop. Errors
// are logged but do not block shutdown.
func (c *Channel) flushCursorOnExit(parentCtx context.Context) {
	if !c.cursor.IsDirty() {
		return
	}
	ctx, cancel := context.WithTimeout(parentCtx, 5*time.Second)
	defer cancel()
	if err := c.flushCursor(ctx); err != nil {
		slog.Warn("zalo_oa.poll.cursor_flush_on_exit_failed", "error", err)
	}
}
