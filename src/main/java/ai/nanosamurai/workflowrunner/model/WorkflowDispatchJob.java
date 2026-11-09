package ai.nanosamurai.workflowrunner.model;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.time.Instant;
import java.util.List;

@JsonInclude(JsonInclude.Include.NON_NULL)
public record WorkflowDispatchJob(
        @JsonProperty("schema_version") int schemaVersion,
        @JsonProperty("workflow_run_id") String workflowRunId,
        @JsonProperty("tenant_id") String tenantId,
        @JsonProperty("session_id") String sessionId,
        @JsonProperty("workflow_id") String workflowId,
        Trigger trigger,
        Provider provider,
        Prompt prompt,
        Input input,
        Stream stream,
        Attempt attempt
) {
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public record Trigger(
            String type,
            @JsonProperty("track_id") String trackId,
            @JsonProperty("source_event_id") String sourceEventId,
            @JsonProperty("created_at") Instant createdAt
    ) {
    }

    @JsonInclude(JsonInclude.Include.NON_NULL)
    public record Provider(
            String type,
            @JsonProperty("model_id") String modelId,
            Object params
    ) {
    }

    @JsonInclude(JsonInclude.Include.NON_NULL)
    public record Prompt(
            String text
    ) {
    }

    @JsonInclude(JsonInclude.Include.NON_NULL)
    public record Input(
            Transcript transcript
    ) {
        @JsonInclude(JsonInclude.Include.NON_NULL)
        public record Transcript(
                String kind,
                List<Segment> segments
        ) {
            @JsonInclude(JsonInclude.Include.NON_NULL)
            public record Segment(
                    @JsonProperty("start_s") Double startS,
                    @JsonProperty("end_s") Double endS,
                    String speaker,
                    String text
            ) {
            }
        }
    }

    @JsonInclude(JsonInclude.Include.NON_NULL)
    public record Stream(
            @JsonProperty("source_uri") String sourceUri,
            @JsonProperty("source_node_id") String sourceNodeId
    ) {
    }

    @JsonInclude(JsonInclude.Include.NON_NULL)
    public record Attempt(
            @JsonProperty("no") int no
    ) {
    }
}
