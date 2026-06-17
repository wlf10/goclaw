
		// Echo reasoning_content for APIs/models that require it on every assistant
		// message. DeepSeek thinking mode returns HTTP 400 if any assistant message
		// lacks the field after a tool call, even when empty. Together/Qwen gateways
		// reject unknown fields; the allowlist in openAIWireAssistantReasoningContent
		// ensures we only emit for compatible APIs.
		if m.Role == "assistant" && openAIWireAssistantReasoningContent(model) {
			if m.Thinking != "" {
				msg["reasoning_content"] = m.Thinking
			} else {
				msg["reasoning_content"] = ""
			}
		}

