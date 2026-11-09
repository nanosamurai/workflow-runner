package ai.nanosamurai.workflowrunner.model;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.time.Instant;

@JsonInclude(JsonInclude.Include.NON_NULL)
public record WorkflowResult(
        @JsonProperty("schema_version") int schemaVersion,
        @JsonProperty("workflow_run_id") String workflowRunId,
        @JsonProperty("tenant_id") String tenantId,
        @JsonProperty("session_id") String sessionId,
        @JsonProperty("workflow_id") String workflowId,
        WorkflowDispatchJob.Trigger trigger,
        String status,
        Render render,
        WorkflowDispatchJob.Provider provider,
        Usage usage,
        @JsonProperty("created_at") Instant createdAt,
        WorkflowDispatchJob.Stream stream,
        @JsonProperty("error_code") String errorCode,
        @JsonProperty("error_detail") String errorDetail
) {
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public record Render(
            String markdown,
            Object json
    ) {
    }

    @JsonInclude(JsonInclude.Include.NON_NULL)
    public record Usage(
            @JsonProperty("input_tokens") Integer inputTokens,
            @JsonProperty("output_tokens") Integer outputTokens
    ) {
    }
}
