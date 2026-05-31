package channels

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/nextlevelbuilder/goclaw/internal/bus"
	"github.com/nextlevelbuilder/goclaw/pkg/protocol"
)

func TestHandleAgentEvent_QuickAckNonStreamingOnly(t *testing.T) {
	behavior := ResolvedChatBehavior{
		Enabled: true,
		QuickAck: ResolvedQuickAckConfig{
			Enabled:    true,
			MinDelayMs: 0,
			Templates:  []string{"On it."},
		},
	}

	mb := bus.New()
	mgr := NewManager(mb)
	mgr.RegisterChannel("test", &chatBehaviorTestChannel{name: "test"})
	mgr.RegisterRunWithBehavior("run-1", "test", "chat-1", "msg-1", map[string]string{"local_key": "chat-1/topic"}, uuid.Nil, false, false, true, behavior)

	mgr.HandleAgentEvent(protocol.AgentEventRunStarted, "run-1", nil)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	got, ok := mb.SubscribeOutbound(ctx)
	if !ok {
		t.Fatal("expected quick acknowledgement outbound message")
	}
	if got.Content != "On it." || got.ChatID != "chat-1" || got.Metadata["local_key"] != "chat-1/topic" {
		t.Fatalf("quick ack outbound = %+v, want content and routing metadata", got)
	}

	mb = bus.New()
	mgr = NewManager(mb)
	mgr.RegisterChannel("test", &chatBehaviorTestChannel{name: "test"})
	mgr.RegisterRunWithBehavior("run-2", "test", "chat-1", "msg-1", nil, uuid.Nil, true, false, true, behavior)

	mgr.HandleAgentEvent(protocol.AgentEventRunStarted, "run-2", nil)

	ctx, cancel = context.WithTimeout(context.Background(), 25*time.Millisecond)
	defer cancel()
	if got, ok := mb.SubscribeOutbound(ctx); ok {
		t.Fatalf("streaming run emitted quick ack: %+v", got)
	}
}

func TestUnregisterRun_CancelsPendingQuickAck(t *testing.T) {
	mb := bus.New()
	mgr := NewManager(mb)
	mgr.RegisterChannel("test", &chatBehaviorTestChannel{name: "test"})
	mgr.RegisterRunWithBehavior("run-1", "test", "chat-1", "msg-1", nil, uuid.Nil, false, false, true, ResolvedChatBehavior{
		Enabled: true,
		QuickAck: ResolvedQuickAckConfig{
			Enabled:    true,
			MinDelayMs: 50,
			Templates:  []string{"On it."},
		},
	})

	mgr.HandleAgentEvent(protocol.AgentEventRunStarted, "run-1", nil)
	mgr.UnregisterRun("run-1")

	ctx, cancel := context.WithTimeout(context.Background(), 75*time.Millisecond)
	defer cancel()
	if got, ok := mb.SubscribeOutbound(ctx); ok {
		t.Fatalf("unregistered run emitted quick ack: %+v", got)
	}
}

func TestCancelQuickAck_BlocksInFlightSend(t *testing.T) {
	mb := bus.New()
	mgr := NewManager(mb)
	rc := &RunContext{
		ChannelName: "test",
		ChatID:      "chat-1",
		ChatBehavior: ResolvedChatBehavior{
			Enabled: true,
			QuickAck: ResolvedQuickAckConfig{
				Enabled:   true,
				Templates: []string{"On it."},
			},
		},
	}

	mgr.cancelQuickAck(rc)
	mgr.sendQuickAck(rc)

	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Millisecond)
	defer cancel()
	if got, ok := mb.SubscribeOutbound(ctx); ok {
		t.Fatalf("cancelled quick ack emitted message: %+v", got)
	}
}
