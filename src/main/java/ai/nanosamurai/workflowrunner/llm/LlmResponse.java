package ai.nanosamurai.workflowrunner.llm;

import com.fasterxml.jackson.annotation.JsonInclude;

@JsonInclude(JsonInclude.Include.NON_NULL)
public record LlmResponse(
        String text,
        Integer inputTokens,
        Integer outputTokens
) {
}
