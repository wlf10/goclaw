package pipeline

import (
	"context"
	"fmt"
	"log/slog"
	"strings"

	"github.com/nextlevelbuilder/goclaw/internal/providers"
)

const maxTruncRetries = 3

// ThinkStage runs per iteration. Calls LLM, handles truncation retries,
// accumulates usage, returns BreakLoop when response has no tool calls.
type ThinkStage struct {
	deps   *PipelineDeps
	result StageResult
}

// NewThinkStage creates a ThinkStage.
func NewThinkStage(deps *PipelineDeps) *ThinkStage {
	return &ThinkStage{deps: deps, result: Continue}
}

func (s *ThinkStage) Name() string       { return "think" }
func (s *ThinkStage) Result() StageResult { return s.result }

// Execute builds tools, calls LLM, handles truncation, sets flow control.
func (s *ThinkStage) Execute(ctx context.Context, state *RunState) error {
	s.result = Continue

	// 1. Iteration budget nudges (70% / 90%)
	s.maybeInjectNudge(state)

	// 2. Build filtered tool definitions
	var toolDefs []providers.ToolDefinition
	if s.deps.BuildFilteredTools != nil {
		var err error
		toolDefs, err = s.deps.BuildFilteredTools(state)
		if err != nil {
			return fmt.Errorf("build tools: %w", err)
		}
	}

	// 3. Construct ChatRequest
	req := providers.ChatRequest{
		Messages: state.Messages.All(),
		Tools:    toolDefs,
		Model:    state.Model,
		Options: map[string]any{
			providers.OptMaxTokens: s.deps.Config.MaxTokens,
		},
	}

	// 4. Call LLM (stream or sync — delegated to callback)
	if s.deps.CallLLM == nil {
		return fmt.Errorf("CallLLM callback not configured")
	}
	resp, err := s.deps.CallLLM(ctx, state, req)
	if err != nil {
		// Issue 958: Check for context overflow — attempt emergency compaction + retry
		if isContextOverflowErr(err) {
			if state.Think.OverflowRetries > 0 {
				return fmt.Errorf("context overflow after compaction: %w", err)
			}
			state.Think.OverflowRetries++
			// Attempt emergency compaction
			if s.deps.CompactMessages != nil {
				originalLen := len(state.Messages.History())
				compacted, compactErr := s.deps.CompactMessages(ctx, state.Messages.History(), state.Model)
				if compactErr == nil {
					state.Messages.ReplaceHistory(compacted)
					slog.Info("emergency_compaction_triggered",
						"run_id", state.RunID,
						"original_msgs", originalLen,
						"compacted_msgs", len(compacted),
					)
					return nil // Retry this iteration (Continue result)
				}
				slog.Warn("emergency_compaction_failed", "error", compactErr)
			}
		}
		return fmt.Errorf("llm call: %w", err)
	}
	state.Think.LastResponse = resp

	// 5. Accumulate usage (including ThinkingTokens for reasoning models)
	if resp.Usage != nil {
		state.Think.TotalUsage.PromptTokens += resp.Usage.PromptTokens
		state.Think.TotalUsage.CompletionTokens += resp.Usage.CompletionTokens
		state.Think.TotalUsage.TotalTokens += resp.Usage.TotalTokens
		state.Think.TotalUsage.ThinkingTokens += resp.Usage.ThinkingTokens
	}

	// 6. Handle truncation: retry when tool call args are truncated or malformed.
	truncated := len(resp.ToolCalls) > 0 && (resp.FinishReason == "length" ||
		(resp.FinishReason == "tool_calls" && toolCallsHaveMissingRequiredArgs(resp.ToolCalls)))
	parseErr := !truncated && toolCallsHaveParseErrors(resp.ToolCalls)
	if truncated || parseErr {
		state.Think.TruncRetries++
		if state.Think.TruncRetries >= maxTruncRetries {
			s.result = AbortRun
			return nil
		}
		hint := "[System] Your output was truncated because it exceeded max_tokens."
		if parseErr {
			hint = "[System] One or more tool call arguments were malformed (truncated JSON)."
		}
		state.Messages.AppendPending(providers.Message{
			Role:     "assistant",
			Content:  resp.Content,
			Thinking: resp.Thinking,
		})
		state.Messages.AppendPending(providers.Message{Role: "user", Content: hint})
		return nil
	}
	state.Think.TruncRetries = 0
	state.Think.OverflowRetries = 0

	// 7. Uniquify tool call IDs.
	if len(resp.ToolCalls) > 0 && resp.RawAssistantContent == nil && s.deps.UniqueToolCallIDs != nil {
		resp.ToolCalls = s.deps.UniqueToolCallIDs(resp.ToolCalls, state.RunID, state.Iteration)
	}

	// 8. Flow control + message append.
	if len(resp.ToolCalls) == 0 {
		s.result = BreakLoop
		return nil
	}

	assistantMsg := providers.Message{
		Role:                "assistant",
		Content:             resp.Content,
		Thinking:            resp.Thinking,
		ToolCalls:           resp.ToolCalls,
		Phase:               resp.Phase,
		RawAssistantContent: resp.RawAssistantContent,
	}
	state.Messages.AppendPending(assistantMsg)

	if resp.Content != "" && s.deps.EmitBlockReply != nil {
		s.deps.EmitBlockReply(resp.Content)
	}

	return nil
}

func (s *ThinkStage) maybeInjectNudge(state *RunState) {
	maxIter := s.deps.Config.MaxIterations
	if maxIter <= 0 {
		return
	}
	pct := float64(state.Iteration) / float64(maxIter)
	if pct >= 0.9 && !state.Evolution.Nudge90Sent {
		state.Evolution.Nudge90Sent = true
		state.Messages.AppendPending(providers.Message{
			Role:    "user",
			Content: "[System] URGENT: At 90% of iteration budget. Deliver results now.",
		})
	} else if pct >= 0.7 && !state.Evolution.Nudge70Sent {
		state.Evolution.Nudge70Sent = true
		state.Messages.AppendPending(providers.Message{
			Role:    "user",
			Content: "[System] At 70% of iteration budget. Start wrapping up.",
		})
	}
}

func toolCallsHaveParseErrors(calls []providers.ToolCall) bool {
	for _, tc := range calls {
		if tc.ParseError != "" {
			return true
		}
	}
	return false
}

var mutatingToolsRequireArgs = map[string]struct{}{
	"write_file":   {},
	"edit":         {},
	"exec":         {},
	"create_image": {},
	"read_file":    {},
}

func toolCallsHaveMissingRequiredArgs(calls []providers.ToolCall) bool {
	for _, tc := range calls {
		if _, requires := mutatingToolsRequireArgs[tc.Name]; !requires {
			continue
		}
		if len(tc.Arguments) == 0 {
			return true
		}
	}
	return false
}

func isContextOverflowErr(err error) bool {
	if err == nil {
		return false
	}
	lower := strings.ToLower(err.Error())
	return providers.IsContextOverflowMessage(lower)
}
