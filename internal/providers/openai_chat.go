package providers

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"strings"
)

func (p *OpenAIProvider) Chat(ctx context.Context, req ChatRequest) (*ChatResponse, error) {
	model := p.resolveModel(req.Model)
	body := p.buildRequestBody(model, req, false)
	body = ApplyMiddlewares(body, p.middlewares, p.middlewareConfig(model, req))
	chatFn := p.chatRequestFn(ctx, body)
	resp, err := RetryDo(ctx, p.retryConfig, chatFn)
	if err != nil {
		if clamped := clampMaxTokensFromError(err, body); clamped {
			slog.Info("max_tokens clamped, retrying", "model", model, "limit", clampedLimit(body))
			resp, err = RetryDo(ctx, p.retryConfig, chatFn)
		}
	}
	// OptStripThinking controls user-facing thinking events - must NOT clear
	// resp.Thinking because leaker models require reasoning_content echo.
	return resp, err
}

func (p *OpenAIProvider) ChatStream(ctx context.Context, req ChatRequest, onChunk func(StreamChunk)) (*ChatResponse, error) {
	model := p.resolveModel(req.Model)
	stripThinking, _ := req.Options[OptStripThinking].(bool)
	body := p.buildRequestBody(model, req, true)
	body = ApplyMiddlewares(body, p.middlewares, p.middlewareConfig(model, req))
	respBody, err := RetryDo(ctx, p.retryConfig, func() (io.ReadCloser, error) {
		return p.doRequest(ctx, body)
	})
	if err != nil {
		if clamped := clampMaxTokensFromError(err, body); clamped {
			respBody, err = RetryDo(ctx, p.retryConfig, func() (io.ReadCloser, error) {
				return p.doRequest(ctx, body)
			})
		}
	}
	if err != nil { return nil, err }
	cb := NewCtxBody(ctx, respBody)
	defer cb.Close()
	result := &ChatResponse{FinishReason: "stop"}
	accumulators := make(map[int]*toolCallAccumulator)
	sse := NewSSEScanner(cb)
	for sse.Next() {
		data := sse.Data()
		var chunk openAIStreamChunk
		if err := json.Unmarshal([]byte(data), &chunk); err != nil { continue }
		if chunk.Usage != nil {
			result.Usage = &Usage{
				PromptTokens: chunk.Usage.PromptTokens,
				CompletionTokens: chunk.Usage.CompletionTokens,
				TotalTokens: chunk.Usage.TotalTokens,
			}
		}
		if len(chunk.Choices) == 0 { continue }
		delta := chunk.Choices[0].Delta
		reasoning := delta.ReasoningContent
		if reasoning == "" { reasoning = delta.Reasoning }
		if reasoning != "" {
			result.Thinking += reasoning
			if !stripThinking && onChunk != nil {
				onChunk(StreamChunk{Thinking: reasoning})
			}
		}
		if delta.Content != "" {
			result.Content += delta.Content
			if onChunk != nil { onChunk(StreamChunk{Content: delta.Content}) }
		}
		for _, img := range delta.Images {
			mimeType, b64Data, err := parseDataURL(img.ImageURL.URL)
			if err != nil { continue }
			result.Images = append(result.Images, ImageContent{MimeType: mimeType, Data: b64Data})
		}
		for _, tc := range delta.ToolCalls {
			acc, ok := accumulators[tc.Index]
			if !ok {
				acc = &toolCallAccumulator{
					ToolCall: ToolCall{ID: tc.ID, Name: strings.TrimSpace(tc.Function.Name)},
				}
				accumulators[tc.Index] = acc
			}
			if tc.Function.Name != "" { acc.Name = strings.TrimSpace(tc.Function.Name) }
			acc.rawArgs += tc.Function.Arguments
		}
		if chunk.Choices[0].FinishReason != "" {
			result.FinishReason = chunk.Choices[0].FinishReason
		}
	}
	if err := sse.Err(); err != nil {
		return nil, fmt.Errorf("%s: stream read error: %w", p.name, err)
	}
	for i := 0; i < len(accumulators); i++ {
		acc := accumulators[i]
		args := make(map[string]any)
		if err := json.Unmarshal([]byte(acc.rawArgs), &args); err != nil && acc.rawArgs != "" {
			acc.ParseError = fmt.Sprintf("malformed JSON (%d chars): %v", len(acc.rawArgs), err)
		}
		acc.Arguments = args
		result.ToolCalls = append(result.ToolCalls, acc.ToolCall)
	}
	if len(result.ToolCalls) > 0 && result.FinishReason != "length" {
		result.FinishReason = "tool_calls"
	}
	if onChunk != nil { onChunk(StreamChunk{Done: true}) }
	return result, nil
}

func normalizeMistralToolCallID(id string) string {
	h := sha256.Sum256([]byte(id))
	return hex.EncodeToString(h[:])[:9]
}

func (p *OpenAIProvider) wireToolCallID(id string) string {
	if p.name == "mistral" || p.providerType == "mistral" {
		return normalizeMistralToolCallID(id)
	}
	return truncateToolCallID(id)
}

func truncateToolCallID(id string) string {
	if len(id) <= maxToolCallIDLen { return id }
	hash := sha256.Sum256([]byte(id))
	return "call_" + hex.EncodeToString(hash[:])[:maxToolCallIDLen-len("call_")]
}

const maxToolCallIDLen = 40
