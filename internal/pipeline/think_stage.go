package pipeline

import (
	"fmt"

	"github.com/wlf10/goclaw/internal/providers"
)

// ThinkStage retries the LLM call when the response was truncated (finish_reason="length").
type ThinkStage struct {
	maxRetries int
}

func NewThinkStage(maxRetries int) *ThinkStage {
	return &ThinkStage{maxRetries: maxRetries}
}

func (s *ThinkStage) Name() string {
	return "think"
}

func (s *ThinkStage) IsRequired(state *RunState) bool {
	return false
}

func (s *ThinkStage) Execute(ctx context.Context, state *RunState) error {
	if state.Response == nil || state.Response.FinishReason != "length" {
		return nil
	}

	resp := state.Response
	if !hasToolCalls(state.Messages.All()) {
		return nil
	}

	retries := 0
	if s.maxRetries > 0 {
		retries = s.maxRetries
	}

	for i := 0; i <= retries; i++ {
		state.Iteration++

		chatReq := *state.ChatRequest
		resp, err := state.Provider.Chat(ctx, &chatReq)
		if err != nil {
			return fmt.Errorf("think stage retry %d: %w", i, err)
		}

		if resp.FinishReason != "length" {
			state.Response = resp
			return nil
		}

		_ = resp
	}

	// Truncation retry exhausted — build hint and retry with preserved thinking + tool calls
	resp = state.Response
	hint := "[System] The response was truncated. Please continue from where you left off."
	if resp != nil {
		for _, tc := range resp.ToolCalls {
			if tc.Function != nil && !strings.HasSuffix(tc.Function.Arguments, "}") {
				hint = "[System] One or more tool call arguments were malformed (truncated JSON). Please retry with shorter content."
				break
			}
		}
	}
	state.Messages.AppendPending(providers.Message{
		Role:      "assistant",
		Content:   resp.Content,
		Thinking:  resp.Thinking,
		ToolCalls: resp.ToolCalls,
	})
	state.Messages.AppendPending(providers.Message{Role: "user", Content: hint})
	return nil
}