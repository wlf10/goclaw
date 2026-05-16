package agent

import (
	"context"
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
		event.MessageID = req.MessageID
		event.TenantID = l.tenantID
		l.emit(event)
	}
	return pipelineCallbackSet{
		emitRun: emitRun, injectContext: l.makeInjectContext(req),
		loadSessionHistory: l.makeLoadSessionHistory(),
		resolveWorkspace: l.makeResolveWorkspace(req),
		loadContextFiles: l.makeLoadContextFiles(),
		buildMessages: l.makeBuildMessages(),
		enrichMedia: l.makeEnrichMedia(req),
		injectReminders: l.makeInjectReminders(req),
		buildFilteredTools: l.makeBuildFilteredTools(req),
		callLLM: l.makeCallLLM(req, emitRun),
		pruneMessages: l.makePruneMessages(),
		sanitizeHistory: sanitizeHistory,
		compactMessages: l.makeCompactMessages(req),
		runMemoryFlush: l.makeRunMemoryFlush(),
		executeToolCall: l.makeExecuteToolCall(req, bridgeRS),
		executeToolRaw: l.makeExecuteToolRaw(req),
		processToolResult: l.makeProcessToolResult(req, bridgeRS),
		checkReadOnly: l.makeCheckReadOnly(req, bridgeRS),
		sanitizeContent: SanitizeAssistantContent,
		flushMessages: l.makeFlushMessages(req),
		updateMetadata: l.makeUpdateMetadata(req),
		bootstrapCleanup: l.makeBootstrapCleanup(),
		maybeSummarize: l.maybeSummarize,
	}
}

type pipelineCallbackSet struct {
	emitRun func(AgentEvent)
	injectContext func(ctx context.Context, input *pipeline.RunInput) (context.Context, error)
	loadSessionHistory func(ctx context.Context, sessionKey string) ([]providers.Message, string)
	resolveWorkspace func(ctx context.Context, input *pipeline.RunInput) (*workspace.WorkspaceContext, error)
	loadContextFiles func(ctx context.Context, userID string) ([]bootstrap.ContextFile, bool)
	buildMessages func(ctx context.Context, input *pipeline.RunInput, history []providers.Message, summary string) ([]providers.Message, error)
	enrichMedia func(ctx context.Context, state *pipeline.RunState) error
	injectReminders func(ctx context.Context, input *pipeline.RunInput, msgs []providers.Message) []providers.Message
	buildFilteredTools func(state *pipeline.RunState) ([]providers.ToolDefinition, error)
	callLLM func(ctx context.Context, state *pipeline.RunState, req providers.ChatRequest) (*providers.ChatResponse, error)
	pruneMessages func(msgs []providers.Message, budget int) ([]providers.Message, pipeline.PruneStats)
	sanitizeHistory func(msgs []providers.Message) ([]providers.Message, int)
	compactMessages func(ctx context.Context, msgs []providers.Message, model string) ([]providers.Message, error)
	runMemoryFlush func(ctx context.Context, state *pipeline.RunState) error
	executeToolCall func(ctx context.Context, state *pipeline.RunState, tc providers.ToolCall) ([]providers.Message, error)
	executeToolRaw func(ctx context.Context, tc providers.ToolCall) (providers.Message, any, error)
	processToolResult func(ctx context.Context, state *pipeline.RunState, tc providers.ToolCall, rawMsg providers.Message, rawData any) []providers.Message
	checkReadOnly func(state *pipeline.RunState) (*providers.Message, bool)
	sanitizeContent func(string) string
	flushMessages func(ctx context.Context, sessionKey string, msgs []providers.Message) error
	updateMetadata func(ctx context.Context, sessionKey string, usage providers.Usage) error
	bootstrapCleanup func(ctx context.Context, state *pipeline.RunState) error
	maybeSummarize func(ctx context.Context, sessionKey string)
}

func (l *Loop) makeResolveWorkspace(req *RunRequest) func(ctx context.Context, input *pipeline.RunInput) (*workspace.WorkspaceContext, error) {
	resolver := workspace.NewResolver()
	return func(ctx context.Context, input *pipeline.RunInput) (*workspace.WorkspaceContext, error) {
		var teamID *string
		if input.TeamID != "" {
			teamID = &input.TeamID
		}
		return resolver.Resolve(ctx, workspace.ResolveParams{
			AgentID: l.id, AgentType: l.agentType,
			UserID: input.UserID, ChatID: input.ChatID,
			TenantID: l.tenantID.String(), PeerKind: input.PeerKind,
			TeamID: teamID, BaseDir: l.workspace,
		})
	}
}

func (l *Loop) makeBuildMessages() func(ctx context.Context, input *pipeline.RunInput, history []providers.Message, summary string) ([]providers.Message, error) {
	return func(ctx context.Context, input *pipeline.RunInput, history []providers.Message, summary string) ([]providers.Message, error) {
		msgs, _ := l.buildMessages(ctx, history, summary,
			input.Message, input.ExtraSystemPrompt,
			input.SessionKey, input.Channel, input.ChannelType,
			input.ChatTitle, input.ChatID, input.PeerKind, input.UserID,
			input.HistoryLimit, input.SkillFilter, input.LightContext)
		return msgs, nil
	}
}

func (l *Loop) makeInjectContext(req *RunRequest) func(ctx context.Context, input *pipeline.RunInput) (context.Context, error) {
	return func(ctx context.Context, input *pipeline.RunInput) (context.Context, error) {
		result, err := l.injectContext(ctx, req)
		if err != nil { return ctx, err }
		input.Message = req.Message
		if l.sessions.GetContextWindow(result.ctx, req.SessionKey) <= 0 {
			l.sessions.SetContextWindow(result.ctx, req.SessionKey, l.contextWindow)
		}
		return result.ctx, nil
	}
}

func (l *Loop) makeCallLLM(req *RunRequest, emitRun func(AgentEvent)) func(ctx context.Context, state *pipeline.RunState, chatReq providers.ChatRequest) (*providers.ChatResponse, error) {
	return func(ctx context.Context, state *pipeline.RunState, chatReq providers.ChatRequest) (*providers.ChatResponse, error) {
		provider := state.Provider
		model := state.Model

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

		reasoningDecision := providers.ResolveReasoningDecision(
			provider, model, l.reasoningConfig.Effort,
			l.reasoningConfig.Fallback, l.reasoningConfig.Source,
		)
		if effort := reasoningDecision.RequestEffort(); effort != "" {
			chatReq.Options[providers.OptThinkingLevel] = effort
		}
		if reasoningDecision.StripThinking {
			chatReq.Options[providers.OptStripThinking] = true
		}

		var resp *providers.ChatResponse
		var err error
		if req.Stream {
			resp, err = provider.ChatStream(ctx, chatReq, func(chunk providers.StreamChunk) {
				if chunk.Thinking != "" {
					emitRun(AgentEvent{Type: protocol.ChatEventThinking, AgentID: l.id, Content: chunk.Thinking})
				}
				if chunk.Content != "" {
					emitRun(AgentEvent{Type: protocol.ChatEventContent, AgentID: l.id, Content: chunk.Content})
				}
				if chunk.ImageURL != "" {
					emitRun(AgentEvent{Type: protocol.ChatEventImage, AgentID: l.id, ImageURL: chunk.ImageURL})
				}
			})
		} else {
			resp, err = provider.Chat(ctx, chatReq)
		}

		if !req.Stream && err == nil && resp != nil {
			stripUserThinking, _ := chatReq.Options[providers.OptStripThinking].(bool)
			if resp.Thinking != "" && !stripUserThinking {
				emitRun(AgentEvent{
					Type: protocol.ChatEventThinking, AgentID: l.id, Content: resp.Thinking,
				})
			}
			if resp.Content != "" {
				emitRun(AgentEvent{Type: protocol.ChatEventContent, AgentID: l.id, Content: resp.Content})
			}
			for _, img := range resp.Images {
				emitRun(AgentEvent{Type: protocol.ChatEventImage, AgentID: l.id, ImageURL: img.ImageURL})
			}
		}

		return resp, err
	}
}

func (l *Loop) makeCompactMessages(req *RunRequest) func(ctx context.Context, msgs []providers.Message, model string) ([]providers.Message, error) {
	return func(ctx context.Context, msgs []providers.Message, model string) ([]providers.Message, error) {
		compacted := l.compactMessagesInPlace(ctx, msgs)
		if compacted == nil {
			return msgs, nil
		}
		return compacted, nil
	}
}

func (l *Loop) makeFlushMessages(req *RunRequest) func(ctx context.Context, sessionKey string, msgs []providers.Message) error {
	var userMsgFlushed bool
	return func(ctx context.Context, sessionKey string, msgs []providers.Message) error {
		if !userMsgFlushed && !req.HideInput && req.Message != "" {
			userMsgFlushed = true
			l.sessions.AddMessage(ctx, sessionKey, providers.Message{Role: "user", Content: req.Message})
		}
		for _, msg := range msgs {
			l.sessions.AddMessage(ctx, sessionKey, msg)
		}
		return nil
	}
}
