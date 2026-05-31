package channels

import (
	"context"
	"reflect"
	"testing"

	"github.com/nextlevelbuilder/goclaw/internal/bus"
	"github.com/nextlevelbuilder/goclaw/internal/config"
)

func TestResolveChatBehavior_InheritsGlobalAndChannelOverride(t *testing.T) {
	global := &config.ChatBehaviorConfig{
		Enabled: new(true),
		QuickAck: &config.QuickAckConfig{
			Enabled:    new(true),
			MinDelayMs: new(750),
			Templates:  []string{"On it."},
		},
		FinalSplit: &config.FinalSplitConfig{
			Enabled:     new(true),
			MinChars:    new(1200),
			MaxMessages: new(3),
			DelayMs:     new(400),
		},
	}
	override := &config.ChatBehaviorConfig{
		QuickAck: &config.QuickAckConfig{Enabled: new(false)},
		FinalSplit: &config.FinalSplitConfig{
			MaxMessages: new(2),
		},
	}

	got := ResolveChatBehavior(global, override)

	if !got.Enabled {
		t.Fatal("Enabled = false, want true")
	}
	if got.QuickAck.Enabled {
		t.Fatal("QuickAck.Enabled = true, want channel override false")
	}
	if got.QuickAck.MinDelayMs != 750 {
		t.Fatalf("QuickAck.MinDelayMs = %d, want 750", got.QuickAck.MinDelayMs)
	}
	if got.FinalSplit.MaxMessages != 2 {
		t.Fatalf("FinalSplit.MaxMessages = %d, want override 2", got.FinalSplit.MaxMessages)
	}
	if got.FinalSplit.MinChars != 1200 || got.FinalSplit.DelayMs != 400 {
		t.Fatalf("FinalSplit inherited fields = %+v, want min=1200 delay=400", got.FinalSplit)
	}
}

func TestSplitFinalMessages_ConservativeParagraphSplit(t *testing.T) {
	cfg := ResolvedFinalSplitConfig{Enabled: true, MinChars: 20, MaxMessages: 3}
	text := "First part is useful.\n\nSecond part is also useful.\n\nThird part closes it."

	got := SplitFinalMessages(text, cfg)
	want := []string{"First part is useful.", "Second part is also useful.", "Third part closes it."}

	if !reflect.DeepEqual(got, want) {
		t.Fatalf("SplitFinalMessages() = %#v, want %#v", got, want)
	}
}

func TestSplitFinalMessages_DoesNotSplitUnsafeMarkdown(t *testing.T) {
	cfg := ResolvedFinalSplitConfig{Enabled: true, MinChars: 10, MaxMessages: 3}
	cases := map[string]string{
		"fenced code":   "Intro.\n\n```go\nfmt.Println(\"hi\")\n```\n\nDone.",
		"table":         "A | B\n--- | ---\n1 | 2\n\nDone.",
		"list":          "Intro.\n\n- one\n- two\n\nDone.",
		"quote":         "Intro.\n\n> quoted\n> text\n\nDone.",
		"json":          "Intro.\n\n{\"ok\": true}\n\nDone.",
		"url paragraph": "Intro.\n\nhttps://example.com/a/b?c=d\n\nDone.",
	}

	for name, text := range cases {
		t.Run(name, func(t *testing.T) {
			got := SplitFinalMessages(text, cfg)
			if len(got) != 1 || got[0] != text {
				t.Fatalf("SplitFinalMessages() = %#v, want original single message", got)
			}
		})
	}
}

func TestPreviewChatBehavior_NoSideEffects(t *testing.T) {
	global := &config.ChatBehaviorConfig{
		Enabled:    new(true),
		QuickAck:   &config.QuickAckConfig{Enabled: new(true), Templates: []string{"Working."}},
		FinalSplit: &config.FinalSplitConfig{Enabled: new(true), MinChars: new(10), MaxMessages: new(2)},
	}

	got := PreviewChatBehavior(global, nil, ChatBehaviorPreviewOptions{
		Content:      "Part one is long.\n\nPart two is long.",
		IsStreaming:  false,
		HasToolCalls: true,
	})

	if !got.Ack.ShouldSend || got.Ack.Content != "Working." {
		t.Fatalf("Ack preview = %+v, want send Working.", got.Ack)
	}
	if len(got.Split.Parts) != 2 {
		t.Fatalf("Split parts = %#v, want two parts", got.Split.Parts)
	}
}

func TestManagerResolveChatBehavior_UsesChannelOverride(t *testing.T) {
	global := &config.ChatBehaviorConfig{
		Enabled:  new(true),
		QuickAck: &config.QuickAckConfig{Enabled: new(true), Templates: []string{"global"}},
	}
	override := &config.ChatBehaviorConfig{
		QuickAck: &config.QuickAckConfig{Enabled: new(true), Templates: []string{"channel"}},
	}
	mgr := NewManager(bus.New())
	mgr.RegisterChannel("test", &chatBehaviorTestChannel{name: "test", behavior: override})

	got := mgr.ResolveChatBehavior("test", global)

	if got.QuickAck.Templates[0] != "channel" {
		t.Fatalf("QuickAck template = %q, want channel override", got.QuickAck.Templates[0])
	}
}

type chatBehaviorTestChannel struct {
	name     string
	behavior *config.ChatBehaviorConfig
}

func (c *chatBehaviorTestChannel) Name() string                                    { return c.name }
func (c *chatBehaviorTestChannel) Type() string                                    { return c.name }
func (c *chatBehaviorTestChannel) Start(context.Context) error                     { return nil }
func (c *chatBehaviorTestChannel) Stop(context.Context) error                      { return nil }
func (c *chatBehaviorTestChannel) Send(context.Context, bus.OutboundMessage) error { return nil }
func (c *chatBehaviorTestChannel) IsRunning() bool                                 { return true }
func (c *chatBehaviorTestChannel) IsAllowed(string) bool                           { return true }
func (c *chatBehaviorTestChannel) ChatBehaviorConfig() *config.ChatBehaviorConfig  { return c.behavior }
