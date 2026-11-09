package ai.nanosamurai.workflowrunner.model;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.time.Instant;

@JsonInclude(JsonInclude.Include.NON_NULL)
public record WorkflowOutcome(
        @JsonProperty("schema_version") int schemaVersion,
        @JsonProperty("workflow_run_id") String workflowRunId,
        @JsonProperty("tenant_id") String tenantId,
        @JsonProperty("session_id") String sessionId,
        @JsonProperty("workflow_id") String workflowId,
        @JsonProperty("attempt_no") int attemptNo,
        WorkflowDispatchJob.Trigger trigger,
        String status,
        @JsonProperty("error_code") String errorCode,
        @JsonProperty("error_detail") String errorDetail,
        @JsonProperty("latency_ms") Long latencyMs,
        @JsonProperty("retry_to_topic") String retryToTopic,
        @JsonProperty("created_at") Instant createdAt,
        WorkflowDispatchJob.Provider provider
) {
    public static WorkflowOutcome from(
            WorkflowDispatchJob job,
            int attemptNo,
            String status,
            String errorCode,
            String errorDetail,
            Long latencyMs,
            String retryToTopic
    ) {
        return new WorkflowOutcome(
                1,
                job == null ? null : job.workflowRunId(),
                job == null ? null : job.tenantId(),
                job == null ? null : job.sessionId(),
                job == null ? null : job.workflowId(),
                attemptNo,
                job == null ? null : job.trigger(),
                status,
                errorCode,
                safeMsg(errorDetail),
                latencyMs,
                retryToTopic,
                Instant.now(),
                job == null ? null : job.provider()
        );
    }

    private static String safeMsg(String s) {
        if (s == null) return null;
        String trimmed = s.replaceAll("[\\r\\n\\t]", " ");
        if (trimmed.length() > 200) {
            return trimmed.substring(0, 200);
        }
        return trimmed;
    }
}
