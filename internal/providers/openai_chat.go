package providers

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"image"
	_ "image/jpeg"
	_ "image/png"
	"io"
	"net/http"
	"strings"
	"time"

	sse "github.com/a-h/stream"
)

func (p *OpenAIProvider) Chat(ctx context.Context, req ChatRequest) (*ChatResponse, error) {
	model := p.resolveModel(req.Model)
	body := p.buildRequestBody(model, req, false)
	body = ApplyMiddlewares(body, p.middlewares, p.middlewareConfig(model, req))

	resp, err := p.chatRequestFn(ctx, body)
	if err != nil {
		return nil, fmt.Errorf("%s chat error: %w", p.name, err)
	}

	// OptStripThinking controls user-facing thinking events (stream chunks,
	// ChatEventThinking) — it must NOT clear resp.Thinking because leaker
	// models (DeepSeek-Reasoner, Kimi) require reasoning_content to be
	// echoed back on subsequent requests. Usage.ThinkingTokens is preserved
	// for billing; user-facing suppression happens in the pipeline callback
	// (loop_pipeline_callbacks.go: makeCallLLM, ChatEventThinking gate).

	return resp, err
}

func (p *OpenAIProvider) chatRequestFn(ctx context.Context, body map[string]any) (*ChatResponse, error) {
	// ... request logic
	return nil, nil
}

func (p *OpenAIProvider) ChatStream(ctx context.Context, req ChatRequest, onChunk func(StreamChunk)) (*ChatResponse, error) {
	model := p.resolveModel(req.Model)
	// stripThinking suppresses user-visible reasoning events, but the
	// content must still be accumulated in result.Thinking for API echoing
	// (DeepSeek requires reasoning_content on subsequent requests).
	stripThinking, _ := req.Options[OptStripThinking].(bool)
	body := p.buildRequestBody(model, req, true)
	body = ApplyMiddlewares(body, p.middlewares, p.middlewareConfig(model, req))

	// ChatRequestFn logic
	return nil, nil
}
