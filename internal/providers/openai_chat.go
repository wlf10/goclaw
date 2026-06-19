
	// OptStripThinking controls user-facing thinking events (stream chunks,
	// ChatEventThinking) — it must NOT clear resp.Thinking because leaker
	// models (DeepSeek-Reasoner, Kimi) require reasoning_content to be
	// echoed back on subsequent requests. Usage.ThinkingTokens is preserved
	// for billing; user-facing suppression happens in the pipeline callback.
	//if resp != nil {
	//	if strip, _ := req.Options[OptStripThinking].(bool); strip {
	//		resp.Thinking = ""
	//	}
	//}
