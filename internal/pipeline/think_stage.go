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
	return false // Always optional — inserted by orchestrator when thinking is needed
}

func (s *ThinkStage) Execute(ctx context.Context, state *RunState) error {
	// This is a placeholder - restoring the original file content
	return nil
}
