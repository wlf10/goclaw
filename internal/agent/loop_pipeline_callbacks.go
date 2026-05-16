// pipelineCallbacks creates all callback closures that capture *Loop.
// Each callback bridges a pipeline.PipelineDeps function to an existing Loop method.
func (l *Loop) pipelineCallbacks(req *RunRequest, bridgeRS *runState) pipelineCallbackSet {
	// Shared emitRun enriches events with routing context (matching v2 pattern).
	emitRun := func(event AgentEvent) {
		event.RunKind = req.RunKind
		event.DelegationID = req.DelegationID
		event.Iteration = bridgeRS.iteration
		event.ToolName = bridgeRS.toolName
		event.ToolInput = bridgeRS.toolInput
		event.ToolResult = bridgeRS.toolResult
		event.ToolStatus = bridgeRS.toolStatus
		event.UserID = req.UserID
		event.Channel = req.Channel
		event.ChatID = req.ChatID
		event.MessageID = req.MessageID
		event.TenantID = l.tenantID
		l.emit(event)
	}

	return pipelineCallbackSet{
		BuildMessages:       l.makeBuildMessages(),
		InjectContext:       l.makeInjectContext(req),
		LoadSessionHistory:  l.makeLoadSessionHistory(),
		EnrichMedia:         l.makeEnrichMedia(req),
		InjectReminders:     l.makeInjectReminders(req),
		BuildFilteredTools:  l.makeBuildFilteredTools(req),
		CallLLM:             l.makeCallLLM(req, emitRun),
		CompactMessages:     l.makeCompactMessages(req),
		RunMemoryFlush:      l.makeRunMemoryFlush(),
		FlushMessages:       l.makeFlushMessages(req),
		UpdateMetadata:      l.makeUpdateMetadata(req),
		SkillPostscript:     l.makeSkillPostscript(),
	}
}

func (l *Loop) makeBuildMessages() func(ctx context.Context, input *pipeline.RunInput) ([]providers.Message, error) {
	return func(ctx context.Context, input *pipeline.RunInput) ([]providers.Message, error) {
		return l.buildMessages(ctx, input)
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

func (l *Loop) makeInjectReminders(req *RunRequest) func(ctx context.Context, input *pipeline.RunInput) {
	return func(ctx context.Context, input *pipeline.RunInput) {
		l.injectReminders(ctx, input)
	}
}

func (l *Loop) makeBuildFilteredTools(req *RunRequest) func(state *pipeline.RunState) ([]providers.ToolDefinition, error) {
	return func(state *pipeline.RunState) ([]providers.ToolDefinition, error) {
		// Load per-user MCP tools (Notion, etc.) into registry before filtering.
		// Servers with require_user_credentials are deferred at startup and
		// connected per-request here with the actual user's credentials.
		l.getUserMCPTools(state.Ctx, state.Input.UserID)
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
		return toolDefs, nil
	}
}

func (l *Loop) makeCallLLM(req *RunRequest, emitRun func(AgentEvent)) func(ctx context.Context, state *pipeline.RunState) error {
	return func(ctx context.Context, state *pipeline.RunState) error {
		chatReq := state.ChatRequest
		provider := state.Provider
		model := state.Model

		// Enrich ChatRequest options to match v2 (providers need these for caching, routing, audit).
		if chatReq.Options == nil {
			chatReq.Options = make(map[string]any)
		}
		chatReq.Options[providers.OptProvider] = provider
		chatReq.Options[providers.OptModel] = model
		chatReq.Options[providers.OptFeatures] = l.features
		chatReq.Options[providers.OptTraceID] = l.traceID
		chatReq.Options[providers.OptUserID] = req.UserID

		if tid := l.tenantID; tid != "" {
			chatReq.Options[providers.OptTenantID] = tid.String()
		}

		// Reasoning decision: resolve effort level for thinking models (o3, DeepSeek-R1, Kimi).
		reasoningDecision := providers.ResolveReasoningDecision(
			provider, model,
			l.reasoningConfig.Effort,
			l.reasoningConfig.MaxTokens,
		)
		chatReq.Options[providers.OptReasoningEffort] = reasoningDecision.Effort
		chatReq.Options[providers.OptReasoningMaxTokens] = reasoningDecision.MaxTokens

		if providers.ResolveStripThinking(provider, model) {
			chatReq.Options[providers.OptStripThinking] = true
		}

		// Emit LLM span start for tracing.
		start := time.Now().UTC()
		var opts []spanOption
		if state.Model != "" {
			opts = append(opts, withModel(state.Model))
		}
		if state.Provider != "" {
			opts = append(opts, withProvider(state.Provider))
		}
		emitRun(AgentEvent{Type: protocol.ChatEventLLMSpanStart, AgentID: l.id})
		llmSpan := l.startLLMSpan("chat", opts...)

		var resp *providers.ChatResponse
		var err error

		if req.Stream {
			resp, err = provider.ChatStream(ctx, *chatReq, func(chunk providers.StreamChunk) {
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

		// Non-streaming: emit content events. Strip thinking events when
		// OptStripThinking is set (leaker models like DeepSeek-Reasoner).
		// resp.Thinking is always preserved internally for API echoing.
		if !req.Stream && err == nil && resp != nil {
			stripUserThinking, _ := chatReq.Options[providers.OptStripThinking].(bool)
			if resp.Thinking != "" && !stripUserThinking {
				emitRun(AgentEvent{
					Type:    protocol.ChatEventThinking,
					AgentID: l.id,
					Content: resp.Thinking,
				})
			}
			if resp.Content != "" {
				emitRun(AgentEvent{Type: protocol.ChatEventContent, AgentID: l.id, Content: resp.Content})
			}
			for _, img := range resp.Images {
				emitRun(AgentEvent{Type: protocol.ChatEventImage, AgentID: l.id, ImageURL: img.ImageURL})
			}
		}

		llmSpan.End()
		state.Response = resp
		state.Err = err
		state.ChatRequest = chatReq
		return nil
	}
}

func (l *Loop) makeCompactMessages(req *RunRequest) func(ctx context.Context, msgs []providers.Message, model string) ([]providers.Message, error) {
	return func(ctx context.Context, msgs []providers.Message, model string) ([]providers.Message, error) {
		compacted := l.compactMessagesInPlace(ctx, msgs)
		if compacted == nil {
			return msgs, nil // compaction failed, return original
		}
		// Stamp session metadata with the compaction timestamp so operators
		// can diagnose compaction cadence without a dedicated column. Stored
		// as RFC3339 string in sessions.metadata JSONB (flushed on next save).
		if l.sessions != nil && req != nil && req.SessionKey != "" {
			l.sessions.SetSessionMetadata(ctx, req.SessionKey, map[string]string{
				SessionMetaKeyLastCompactionAt: time.Now().UTC().Format(time.RFC3339),
			})
		}
		return compacted, nil
	}
}

// SessionMetaKeyLastCompactionAt is the sessions.metadata JSONB key used to
// record the RFC3339 timestamp of the most recent compaction. Exported so
// the web UI code path can read it back via GetSessionMetadata without
// duplicating the string.
const SessionMetaKeyLastCompactionAt = "last_compaction_at"

// cacheTouchAt returns the last prune-mutation timestamp for a session.
// Returns zero time if no touch recorded yet.
func (l *Loop) cacheTouchAt(sessionKey string) time.Time {
	if v, ok := l.cacheTouchBySession.Load(sessionKey); ok {
		return v.(time.Time)
	}
	return time.Time{}
}

// markCacheTouched records the current time as the last prune-mutation timestamp
// for the given session. Called only after pruning actually mutates messages.
func (l *Loop) markCacheTouched(sessionKey string) {
	l.cacheTouchBySession.Store(sessionKey, time.Now())
}

func (l *Loop) makeRunMemoryFlush() func(ctx context.Context, state *pipeline.RunState) {
	return func(ctx context.Context, state *pipeline.RunState) {
		l.runMemoryFlush(ctx, state, l.emit, l.skillEvolve)
	}
}

func (l *Loop) makeFlushMessages(req *RunRequest) func(ctx context.Context, sessionKey string, msgs []providers.Message) error {
	// Track whether user message has been persisted (first flush only).
	// v2 adds user message to pendingMsgs explicitly; v3 keeps it in history
	// (via BuildMessages) so it never reaches FlushPending. This closure
	// persists the user message on first flush to match v2 session format.
	var userMsgFlushed bool
	return func(ctx context.Context, sessionKey string, msgs []providers.Message) error {
		if !userMsgFlushed && !req.HideInput && req.Message != "" {
			l.sessions.AddUserMessage(ctx, sessionKey, req.Message)
			userMsgFlushed = true
		}
		l.sessions.FlushPending(ctx, sessionKey, msgs)
		return nil
	}
}

func (l *Loop) makeUpdateMetadata(req *RunRequest) func(ctx context.Context, sessionKey string, usage providers.Usage) error {
	return func(ctx context.Context, sessionKey string, usage providers.Usage) error {
		l.sessions.UpdateMetadata(ctx, sessionKey, l.model, l.provider.Name(), req.Channel)
		l.sessions.AccumulateTokens(ctx, sessionKey, int64(usage.PromptTokens), int64(usage.CompletionTokens))
		// Persist session to DB (matching v2 finalizeRun behavior).
		// FlushMessages already ran, so all pending messages are in the cache.
		l.sessions.Save(ctx, sessionKey)
		return nil
	}
}

func (l *Loop) makeSkillPostscript() func(ctx context.Context, content string, totalToolCalls int) string {
	if !l.skillEvolve || l.skillNudgeInterval <= 0 {
		return nil // disabled — FinalizeStage skips
	}
	var sent bool
	return func(ctx context.Context, content string, totalToolCalls int) string {
		if sent {
			return ""
		}
		sent = true
		l.runSkillPostscript(ctx, content, totalToolCalls)
		return ""
	}
}

// hasToolCalls checks if any message in the slice contains tool calls.
func hasToolCalls(msgs []providers.Message) bool {
	for _, msg := range msgs {
		if len(msg.ToolCalls) > 0 {
			return true
		}
	}
	return false
}
