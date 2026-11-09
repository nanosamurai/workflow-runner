package ai.nanosamurai.workflowrunner.llm;

import com.fasterxml.jackson.annotation.JsonInclude;

@JsonInclude(JsonInclude.Include.NON_NULL)
public record LlmRequest(
        String modelId,
        Object params,
        String systemPrompt,
        String userPrompt
) {
}
