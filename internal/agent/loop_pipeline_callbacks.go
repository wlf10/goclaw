package agent

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/nextlevelbuilder/goclaw/internal/bootstrap"
	"github.com/nextlevelbuilder/goclaw/internal/config"
	"github.com/nextlevelbuilder/goclaw/internal/i18n"
	"github.com/nextlevelbuilder/goclaw/internal/pipeline"
	"github.com/nextlevelbuilder/goclaw/internal/providers"
	"github.com/nextlevelbuilder/goclaw/internal/store"
	"github.com/nextlevelbuilder/goclaw/internal/tools"
	usagecaps "github.com/nextlevelbuilder/goclaw/internal/usage/caps"
	"github.com/nextlevelbuilder/goclaw/internal/workspace"
	"github.com/nextlevelbuilder/goclaw/pkg/protocol"
)

// pipelineCallbacks creates all callback closures that capture *Loop.
// Each callback bridges a pipeline.PipelineDeps function to an existing Loop method.
func (l *Loop) pipelineCallbacks(req *RunRequest, bridgeRS *runState) pipelineCallbackSet {
	// Shared emitRun enriches events with routing context (matching v2 pattern).
	emitRun := func(event AgentEvent) {
		event.RunKind = req.RunKind
		event.DelegationID = req.DelegationID
		event.TeamID = req.TeamID
		event.TeamTaskID = req.TeamTaskID
		event.ParentAgentID = req.ParentAgentID
		event.SenderID = req.SenderID
		event.UserID = req.UserID
		event.Channel = req.Channel
		event.ChatID = req.ChatID
		event.ChannelType = req.ChannelType
		event.TenantID = l.tenantID
		l.emit(event)
	}
	return pipelineCallbackSet{
		emitRun:            emitRun,
		injectContext:      l.makeInjectContext(req),
		loadSessionHistory: l.makeLoadSessionHistory(),
		resolveWorkspace:   l.makeResolveWorkspace(req),
		loadContextFiles:   l.makeLoadContextFiles(),
		buildMessages:      l.makeBuildMessages(),
		enrichMedia:        l.makeEnrichMedia(req),
		injectReminders:    l.makeInjectReminders(req),
		buildFilteredTools: l.makeBuildFilteredTools(req),
		callLLM:            l.makeCallLLM(req, emitRun),
		pruneMessages:      l.makePruneMessages(),
		sanitizeHistory:    sanitizeHistory,
		compactMessages:    l.makeCompactMessages(req),
		runMemoryFlush:     l.makeRunMemoryFlush(),
		executeToolCall:    l.makeExecuteToolCall(req, bridgeRS),
		executeToolRaw:     l.makeExecuteToolRaw(req),
		processToolResult:  l.makeProcessToolResult(req, bridgeRS),
		authorizeToolCall:  l.makeAuthorizeToolCall(),
		checkReadOnly:      l.makeCheckReadOnly(req, bridgeRS),
		sanitizeContent:    SanitizeAssistantContent,
		flushMessages:      l.makeFlushMessages(req),
		updateMetadata:     l.makeUpdateMetadata(req),
		bootstrapCleanup:   l.makeBootstrapCleanup(),
		maybeSummarize:     l.maybeSummarize,
	}
}

// pipelineCallbackSet groups all typed callbacks for PipelineDeps.
type pipelineCallbackSet struct {
	emitRun            func(AgentEvent)
	injectContext      func(ctx context.Context, input *pipeline.RunInput) (context.Context, error)
	loadSessionHistory func(ctx context.Context, sessionKey string) ([]providers.Message, string)
	resolveWorkspace   func(ctx context.Context, input *pipeline.RunInput) (*workspace.WorkspaceContext, error)
	loadContextFiles   func(ctx context.Context, userID string) ([]bootstrap.ContextFile, bool)
	buildMessages      func(ctx context.Context, input *pipeline.RunInput, history []providers.Message, summary string) ([]providers.Message, error)
	enrichMedia        func(ctx context.Context, state *pipeline.RunState) error
	injectReminders    func(ctx context.Context, input *pipeline.RunInput, msgs []providers.Message) []providers.Message
	buildFilteredTools func(state *pipeline.RunState) ([]providers.ToolDefinition, error)
	callLLM            func(ctx context.Context, state *pipeline.RunState, req providers.ChatRequest) (*providers.ChatResponse, error)
	pruneMessages      func(msgs []providers.Message, budget int) ([]providers.Message, pipeline.PruneStats)
	sanitizeHistory    func(msgs []providers.Message) ([]providers.Message, int)
	compactMessages    func(ctx context.Context, msgs []providers.Message, model string) ([]providers.Message, error)
	runMemoryFlush     func(ctx context.Context, state *pipeline.RunState) error
	executeToolCall    func(ctx context.Context, state *pipeline.RunState, tc providers.ToolCall) ([]providers.Message, error)
	executeToolRaw     func(ctx context.Context, tc providers.ToolCall) (providers.Message, any, error)
	processToolResult  func(ctx context.Context, state *pipeline.RunState, tc providers.ToolCall, rawMsg providers.Message, rawData any) []providers.Message
	authorizeToolCall  func(ctx context.Context, state *pipeline.RunState, tc providers.ToolCall) (bool, string)
	checkReadOnly      func(state *pipeline.RunState) (*providers.Message, bool)
	sanitizeContent    func(string) string
	flushMessages      func(ctx context.Context, sessionKey string, msgs []providers.Message) error
	updateMetadata     func(ctx context.Context, sessionKey string, usage providers.Usage) error
	bootstrapCleanup   func(ctx context.Context, state *pipeline.RunState) error
	maybeSummarize     func(ctx context.Context, sessionKey string)
}

func (l *Loop) makeResolveWorkspace(req *RunRequest) func(ctx context.Context, input *pipeline.RunInput) (*workspace.WorkspaceContext, error) {
	resolver := workspace.NewResolver()
	return func(ctx context.Context, input *pipeline.RunInput) (*workspace.WorkspaceContext, error) {
		var teamID *string
		if input.TeamID != "" {
			teamID = &input.TeamID
		}
		return resolver.Resolve(ctx, workspace.ResolveParams{
			AgentID:   l.id,
			AgentType: l.agentType,
			UserID:    input.UserID,
			ChatID:    input.ChatID,
			TenantID:  l.tenantID.String(),
			PeerKind:  input.PeerKind,
			TeamID:    teamID,
			BaseDir:   l.workspace,
		})
	}
}

func (l *Loop) makeLoadContextFiles() func(ctx context.Context, userID string) ([]bootstrap.ContextFile, bool) {
	return func(ctx context.Context, userID string) ([]bootstrap.ContextFile, bool) {
		files := l.resolveContextFiles(ctx, userID)
		hadBootstrap := false
		for _, f := range files {
			if strings.HasSuffix(f.Path, "BOOTSTRAP.md") {
				hadBootstrap = true
				break
			}
		}
		return files, hadBootstrap
	}
}

func (l *Loop) makeBuildMessages() func(ctx context.Context, input *pipeline.RunInput, history []providers.Message, summary string) ([]providers.Message, error) {
	return func(ctx context.Context, input *pipeline.RunInput, history []providers.Message, summary string) ([]providers.Message, error) {
		msgs, _ := l.buildMessages(ctx, history, summary,
			input.Message, input.ExtraSystemPrompt,
			input.SessionKey, input.Channel, input.ChannelType,
			input.BitrixPortalDomain,
			input.ChatTitle, input.ChatID, input.PeerKind, input.UserID, input.SenderName,
			input.HistoryLimit, input.SkillFilter, input.LightContext)
		return msgs, nil
	}
}

// makeInjectContext wraps injectContext() for the v3 pipeline.
// Reuses the existing injectContext() to avoid logic duplication.
// NOTE: injectContext() and this callback must stay in sync when new context values are added.
func (l *Loop) makeInjectContext(req *RunRequest) func(ctx context.Context, input *pipeline.RunInput) (context.Context, error) {
	return func(ctx context.Context, input *pipeline.RunInput) (context.Context, error) {
		result, err := l.injectContext(ctx, req)
		if err != nil {
			return ctx, err
		}
		// Sync message truncation from req back to pipeline input.
		input.Message = req.Message
		// Cache context window on session (first run only).
		if l.sessions.GetContextWindow(result.ctx, req.SessionKey) <= 0 {
			l.sessions.SetContextWindow(result.ctx, req.SessionKey, l.contextWindow)
		}
		return result.ctx, nil
	}
}

// makeLoadSessionHistory loads session history + summary before BuildMessages.
func (l *Loop) makeLoadSessionHistory() func(ctx context.Context, sessionKey string) ([]providers.Message, string) {
	return func(ctx context.Context, sessionKey string) ([]providers.Message, string) {
		history := l.sessions.GetHistory(ctx, sessionKey)
		summary := l.sessions.GetSummary(ctx, sessionKey)
		return history, summary
	}
}

func (l *Loop) makeEnrichMedia(req *RunRequest) func(ctx context.Context, state *pipeline.RunState) error {
	return func(ctx context.Context, state *pipeline.RunState) error {
		// enrichInputMedia enriches messages in-place: attaches inline images,
		// reloads historical media, enriches <media:*> tags, populates context
		// with refs for tool access. Must receive actual messages (not nil) to
		// avoid index-out-of-range panic on inline image attachment.
		msgs := state.Messages.All()
		if len(msgs) == 0 {
			return nil
		}
		enrichedCtx, enrichedMsgs, _ := l.enrichInputMedia(ctx, req, msgs)
		// Propagate enriched context (media images/docs/audio/video refs for tools).
		state.Ctx = enrichedCtx
		// Update history with enriched messages (media tags, inline images).
		// Skip system message (index 0) — only history + user messages are enriched.
		if len(enrichedMsgs) > 1 {
			state.Messages.SetHistory(enrichedMsgs[1:])
		}
		return nil
	}
}

func (l *Loop) makeInjectReminders(req *RunRequest) func(ctx context.Context, input *pipeline.RunInput, msgs []providers.Message) []providers.Message {
	return func(ctx context.Context, input *pipeline.RunInput, msgs []providers.Message) []providers.Message {
		updated, _ := l.injectTeamTaskReminders(ctx, req, msgs)
		return updated
	}
}

func (l *Loop) makeBuildFilteredTools(req *RunRequest) func(state *pipeline.RunState) ([]providers.ToolDefinition, error) {
	return func(state *pipeline.RunState) ([]providers.ToolDefinition, error) {
		// Load per-user MCP tools (Notion, etc.) into registry before filtering.
		// Servers with require_user_credentials are deferred at startup and
		// connected per-request here with the actual user's credentials.
		//
		// Use resolveActorUserID — the gateway consumer rewrites UserID in
		// two scenarios (group chats AND DM with merged contact), both of
		// which break per-user MCP credential lookup. ChannelType discriminates
		// Bitrix24 (always prefer SenderID) from other channels (group-only
		// rewrite recovery). See resolveActorUserID docstring for full rationale.
		actorUserID := resolveActorUserID(
			state.Input.UserID,
			state.Input.SenderID,
			state.Input.PeerKind,
			state.Input.ChannelType,
		)
		userTools := l.getUserMCPTools(state.Ctx, actorUserID)
		slog.Debug("mcp.user_tools_context",
			"peer_kind", state.Input.PeerKind,
			"input_user_id", state.Input.UserID,
			"sender_id", state.Input.SenderID,
			"actor_user_id", actorUserID,
			"user_tools_count", len(userTools))
		maxIter := l.maxIterations
		if req.MaxIterations > 0 && req.MaxIterations < maxIter {
			maxIter = req.MaxIterations
		}
		allMsgs := state.Messages.All()
		toolDefs, _, returnedMsgs := l.buildFilteredTools(req, state.Context.HadBootstrap,
			state.Iteration, maxIter, allMsgs)
		// buildFilteredTools returns the full messages slice; only messages appended
		// beyond the original length are injections (e.g. final-iteration hint).
		// Appending the entire slice would duplicate system+history into pending.
		if len(returnedMsgs) > len(allMsgs) {
			for _, msg := range returnedMsgs[len(allMsgs):] {
				state.Messages.AppendPending(msg)
			}
		}
		mcpDefs := 0
		for _, td := range toolDefs {
			if strings.HasPrefix(strings.TrimSpace(td.Function.Name), "mcp_") {
				mcpDefs++
			}
		}
		slog.Debug("mcp.filtered_tools",
			"tool_defs_count", len(toolDefs),
			"mcp_defs_count", mcpDefs,
			"iteration", state.Iteration)
		return toolDefs, nil
	}
}

// makeAuthorizeToolCall enforces a runtime fail-closed allowlist check before
// every tool execution. AllowedTools is keyed by canonical registry names (built
// by ThinkStage from FilterTools output). The model may emit prefixed names when
// the agent has toolCallPrefix configured (e.g. "proxy_exec" → canonical "exec"),
// so we resolve the name to its canonical form before the lookup to avoid a
// guaranteed miss on every prefixed call.
func (l *Loop) makeAuthorizeToolCall() func(ctx context.Context, state *pipeline.RunState, tc providers.ToolCall) (bool, string) {
	return func(_ context.Context, state *pipeline.RunState, tc providers.ToolCall) (bool, string) {
		allowed := state.Tool.AllowedTools
		if allowed == nil {
			// nil allowlist means no per-iteration restriction (e.g. BuildFilteredTools not wired).
			return true, ""
		}

		// Resolve to canonical name before allowlist lookup. AllowedTools is keyed
		// by canonical names; the model may emit prefixed names when toolCallPrefix
		// is set (e.g. "proxy_exec" vs "exec"). Without this the lookup always misses.
		name := l.resolveToolCallName(tc.Name)

		if allowed[name] {
			return true, ""
		}

		// Preserve lazy activation for deferred tools (typically per-user MCP).
		if l.tools != nil && l.tools.TryActivateDeferred(name) {
			// Re-check deny policy to prevent a lazy-activated tool from bypassing
			// an explicit deny rule.
			if l.toolPolicy != nil && l.toolPolicy.IsDenied(name, l.agentToolPolicy) {
				return false, "tool not allowed by policy: " + name
			}
			allowed[name] = true
			return true, ""
		}

		return false, "tool not allowed by policy: " + name
	}
}

func (l *Loop) makeCallLLM(req *RunRequest, emitRun func(AgentEvent)) func(ctx context.Context, state *pipeline.RunState, chatReq providers.ChatRequest) (*providers.ChatResponse, error) {
	return func(ctx context.Context, state *pipeline.RunState, chatReq providers.ChatRequest) (*providers.ChatResponse, error) {
		provider := state.Provider
		model := state.Model

		// Enrich ChatRequest options to match v2 (providers need these for caching, routing, audit).
		if chatReq.Options == nil {
			chatReq.Options = make(map[string]any)
		}
		chatReq.Options[providers.OptTemperature] = config.DefaultTemperature
		chatReq.Options[providers.OptSessionKey] = req.SessionKey
		chatReq.Options[providers.OptAgentID] = l.agentUUID.String()
		chatReq.Options[providers.OptUserID] = req.UserID
		chatReq.Options[providers.OptChannel] = req.Channel
		chatReq.Options[providers.OptChatID] = req.ChatID
		chatReq.Options[providers.OptPeerKind] = req.PeerKind
		chatReq.Options[providers.OptLocalKey] = req.LocalKey
		chatReq.Options[providers.OptWorkspace] = tools.ToolWorkspaceFromCtx(ctx)
		if tid := store.TenantIDFromContext(ctx); tid != uuid.Nil {
			chatReq.Options[providers.OptTenantID] = tid.String()
		}

		// Reasoning decision: resolve effort level for thinking models (o3, DeepSeek-R1, Kimi).
		reasoningDecision := providers.ResolveReasoningDecision(
			provider, model,
			l.reasoningConfig.Effort,
			l.reasoningConfig.Fallback,
			l.reasoningConfig.Source,
		)
		if effort := reasoningDecision.RequestEffort(); effort != "" {
			chatReq.Options[providers.OptThinkingLevel] = effort
		}
		if reasoningDecision.StripThinking {
			chatReq.Options[providers.OptStripThinking] = true
		}

		// Emit LLM span start for tracing.
		start := time.Now().UTC()
		var opts []spanOption
		if state.Model != "" {
			opts = append(opts, withModel(state.Model))
		}
		if provider != nil {
			opts = append(opts, withProvider(provider.Name()))
		}
		spanID := l.emitLLMSpanStart(ctx, start, state.Iteration+1, chatReq.Messages, opts...)
		recordUsageCapAttempt := func(reservation *usagecaps.Reservation) {
			if reservation != nil {
				opts = append(opts, withUsageCapMetadata(reservation.TraceMetadata()))
			}
		}

		streamThinkingEmitted := false
		emitChunk := func(chunk providers.StreamChunk) {
			if chunk.Thinking != "" {
				streamThinkingEmitted = true
				emitRun(AgentEvent{
					Type:    protocol.ChatEventThinking,
					AgentID: l.id,
					RunID:   req.RunID,
					Payload: map[string]string{"content": chunk.Thinking},
				})
			}
			if chunk.Content != "" {
				emitRun(AgentEvent{
					Type:    protocol.ChatEventChunk,
					AgentID: l.id,
					RunID:   req.RunID,
					Payload: map[string]string{"content": chunk.Content},
				})
			}
		}
		fallbackTraceClassifier := providers.NewDefaultClassifier()
		callProvider := func(attempt string, request providers.ChatRequest) (*providers.ChatResponse, error) {
			if fallbackProvider, ok := provider.(*providers.ModelFallbackProvider); ok {
				before := func(callCtx context.Context, entry providers.FallbackCandidate, actualReq providers.ChatRequest) (providers.FallbackAfterCall, error) {
					candidateAttempt := fmt.Sprintf("%s:%s:%s", attempt, entry.ProviderName, actualReq.Model)
					reservation, reserveErr := l.reserveLLMUsageFor(callCtx, req, state.Iteration, actualReq, candidateAttempt, entry.ProviderName, actualReq.Model)
					if reserveErr != nil {
						recordUsageCapAttempt(reservation)
						return nil, reserveErr
					}
					return func(callResp *providers.ChatResponse, callErr error, info providers.FallbackCallInfo) {
						fallbackMeta := providers.ModelFallbackAttemptMetadata{
							ProviderName: entry.ProviderName,
							Model:        actualReq.Model,
							Status:       "success",
							Streamed:     info.Streamed,
						}
						if callErr != nil {
							classification := providers.ClassifyHTTPError(fallbackTraceClassifier, callErr)
							reason := string(classification.Reason)
							if classification.Kind == "context_overflow" {
								reason = "context_overflow"
							}
							fallbackMeta.Status = "error"
							fallbackMeta.Reason = reason
							fallbackMeta.Error = callErr.Error()
						} else {
							opts = append(opts, withProvider(entry.ProviderName), withModel(actualReq.Model))
						}
						opts = append(opts, withModelFallbackAttempt(fallbackMeta))
						if reservation != nil {
							if info.Streamed {
								reservation.ReconcileStream(callCtx, callResp, callErr, true)
							} else {
								reservation.Reconcile(callCtx, callResp, callErr)
							}
							recordUsageCapAttempt(reservation)
						}
					}, nil
				}
				if req.Stream {
					return fallbackProvider.ChatStreamWithHook(ctx, request, emitChunk, before)
				}
				return fallbackProvider.ChatWithHook(ctx, request, before)
			}
			reservation, reserveErr := l.reserveLLMUsage(ctx, req, state, request, attempt)
			if reserveErr != nil {
				recordUsageCapAttempt(reservation)
				return nil, reserveErr
			}
			var callResp *providers.ChatResponse
			var callErr error
			if req.Stream {
				streamed := false
				callResp, callErr = provider.ChatStream(ctx, request, func(chunk providers.StreamChunk) {
					if chunk.Content != "" || chunk.Thinking != "" || len(chunk.Images) > 0 {
						streamed = true
					}
					emitChunk(chunk)
				})
				if reservation != nil {
					reservation.ReconcileStream(ctx, callResp, callErr, streamed)
					recordUsageCapAttempt(reservation)
				}
				return callResp, callErr
			} else {
				callResp, callErr = provider.Chat(ctx, request)
			}
			if reservation != nil {
				reservation.Reconcile(ctx, callResp, callErr)
				recordUsageCapAttempt(reservation)
			}
			return callResp, callErr
		}

		resp, err := callProvider("initial", chatReq)
		slog.Info("debug.llm.first_response",
			"has_error", err != nil,
			"tool_calls_count", func() int {
				if resp == nil {
					return -1
				}
				return len(resp.ToolCalls)
			}(),
			"tools_provided", len(chatReq.Tools))

		// One guarded retry when MCP task tools are available but the model
		// returns text-only instead of tool calls.
		retryEligible := err == nil && resp != nil && len(resp.ToolCalls) == 0 && shouldRetryTaskMCP(chatReq)
		slog.Info("debug.llm.retry_guard", "retry_eligible", retryEligible)
		if retryEligible {
			retryReq := chatReq
			if retryReq.Options == nil {
				retryReq.Options = make(map[string]any)
			}
			retryReq.Options[providers.OptToolChoice] = "required"
			retryReq.Messages = append(append([]providers.Message{}, chatReq.Messages...), providers.Message{
				Role:    "system",
				Content: "MCP task tools are available in this turn. Do not ask for CRM identifier/email first. Call the relevant MCP task tool immediately, then answer with the tool result.",
			})
			resp, err = callProvider("retry-tool-choice", retryReq)
			slog.Info("debug.llm.retry_response",
				"has_error", err != nil,
				"tool_calls_count", func() int {
					if resp == nil {
						return -1
					}
					return len(resp.ToolCalls)
				}())
		}

		if req.Stream && err == nil && resp != nil && resp.Thinking != "" && !streamThinkingEmitted {
			emitRun(AgentEvent{
				Type:    protocol.ChatEventThinking,
				AgentID: l.id,
				RunID:   req.RunID,
				Payload: map[string]string{"content": resp.Thinking},
			})
		}

		// Non-streaming: emit content events. Strip thinking events when
		// OptStripThinking is set (leaker models like DeepSeek-Reasoner).
		// resp.Thinking is always preserved internally for API echoing.
		if !req.Stream && err == nil && resp != nil {
			stripUserThinking, _ := chatReq.Options[providers.OptStripThinking].(bool)
			if resp.Thinking != "" && !stripUserThinking {
				emitRun(AgentEvent{
					Type:    protocol.ChatEventThinking,
					AgentID: l.id,
					RunID:   req.RunID,
					Payload: map[string]string{"content": resp.Thinking},
				})
			}
			if resp.Content != "" {
				emitRun(AgentEvent{
					Type:    protocol.ChatEventChunk,
					AgentID: l.id,
					RunID:   req.RunID,
					Payload: map[string]string{"content": resp.Content},
				})
			}
		}
		l.emitLLMSpanEnd(ctx, spanID, start, resp, err, opts...)
		return resp, err
	}
}
