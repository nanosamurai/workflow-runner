package ai.nanosamurai.workflowrunner.service;

import ai.nanosamurai.workflowrunner.llm.LlmException;
import ai.nanosamurai.workflowrunner.llm.LlmRequest;
import ai.nanosamurai.workflowrunner.llm.LlmResponse;
import ai.nanosamurai.workflowrunner.llm.LlmRouter;
import ai.nanosamurai.workflowrunner.config.WorkflowRunnerProperties;
import ai.nanosamurai.workflowrunner.model.WorkflowDispatchJob;
import ai.nanosamurai.workflowrunner.model.WorkflowOutcome;
import ai.nanosamurai.workflowrunner.model.WorkflowResult;
import ai.nanosamurai.workflowrunner.render.WorkflowOutputParser;
import ai.nanosamurai.workflowrunner.retry.RetryPlanner;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

public class WorkflowService {
    private static final Logger log = LoggerFactory.getLogger(WorkflowService.class);

    private final WorkflowRunnerProperties props;
    private final KafkaTemplate<String, byte[]> kafka;
    private final ObjectMapper objectMapper;
    private final RetryPlanner retryPlanner;
    private final LlmRouter llmRouter;
    private final WorkflowOutputParser outputParser;

    public WorkflowService(
            WorkflowRunnerProperties props,
            KafkaTemplate<String, byte[]> kafka,
            ObjectMapper objectMapper,
            RetryPlanner retryPlanner,
            LlmRouter llmRouter,
            WorkflowOutputParser outputParser
    ) {
        this.props = props;
        this.kafka = kafka;
        this.objectMapper = objectMapper;
        this.retryPlanner = retryPlanner;
        this.llmRouter = llmRouter;
        this.outputParser = outputParser;
    }

    public HandleResult handle(WorkflowDispatchJob job) {
        int attemptNo = job == null || job.attempt() == null ? 0 : job.attempt().no();

        long startNs = System.nanoTime();
        try {
            validate(job);

            // NOTE: Do not log prompt/transcript bodies.
            log.info("workflow_execution_started provider_type={} model_id={} stream_source_node_id={}",
                    safeMsg(providerType(job)),
                    safeMsg(job.provider().modelId()),
                    job.stream() == null ? null : job.stream().sourceNodeId());
            var llmClient = llmRouter.resolve(providerType(job));

            String systemPrompt = """
                    You are a workflow runner.
                    Return STRICT JSON with shape: {"markdown": <string>, "json": <object|null>}.
                    Do not include any extra keys.
                    """;
            String userPrompt = buildUserPrompt(job);

            LlmResponse resp = llmClient.invoke(new LlmRequest(
                    job.provider().modelId(),
                    job.provider().params(),
                    systemPrompt,
                    userPrompt
            ));

            long latencyMs = (System.nanoTime() - startNs) / 1_000_000L;

            // We got a response back from the LLM provider (success path). Don't log response text.
            log.info("llm_response_received latency_ms={} input_tokens={} output_tokens={}",
                    latencyMs,
                    resp == null ? null : resp.inputTokens(),
                    resp == null ? null : resp.outputTokens());

            WorkflowOutputParser.ParsedOutput parsed = outputParser.parse(resp == null ? null : resp.text());

            // Fallback policy: if output isn't JSON, treat as markdown but include an outcome warning.
            if (!parsed.wasJson()) {
                log.info("workflow_output_fallback reason=OUTPUT_NOT_JSON");
                publishOutcome(WorkflowOutcome.from(job, attemptNo, "SUCCESS", "OUTPUT_NOT_JSON", "Model response was not JSON; using raw text as markdown", latencyMs, null));
            } else {
                publishOutcome(WorkflowOutcome.from(job, attemptNo, "SUCCESS", null, null, latencyMs, null));
            }

            publishResult(new WorkflowResult(
                    1,
                    job.workflowRunId(),
                    job.tenantId(),
                    job.sessionId(),
                    job.workflowId(),
                    job.trigger(),
                    "ok",
                    new WorkflowResult.Render(parsed.markdown(), parsed.json()),
                    resultProvider(job),
                    new WorkflowResult.Usage(resp == null ? null : resp.inputTokens(), resp == null ? null : resp.outputTokens()),
                    Instant.now(),
                    job.stream(),
                    null,
                    null
            ));

            return HandleResult.ack();
        } catch (PermanentFailureException e) {
            return failPermanently(job, attemptNo, e.errorCode(), e.getMessage(), startNs);
        } catch (LlmException e) {
            // Provider messages are short and never contain prompt, transcript or response bodies.
            log.warn("llm_provider_failed provider_type={} error_code={} permanent={} attempt_no={} detail={}",
                    safeMsg(providerType(job)), e.errorCode(), e.permanent(), attemptNo, safeMsg(e.getMessage()));
            if (e.permanent()) {
                return failPermanently(job, attemptNo, e.errorCode(), e.getMessage(), startNs);
            }
            long latencyMs = (System.nanoTime() - startNs) / 1_000_000L;
            return scheduleRetry(job, attemptNo, e.errorCode(), safeMsg(e.getMessage()), latencyMs);
        } catch (Exception e) {
            long latencyMs = (System.nanoTime() - startNs) / 1_000_000L;
            // Treat unknown errors as transient.
            log.warn("Workflow execution failed (will retry if possible): workflow_run_id={} session_id={} tenant_id={} attempt_no={} err={}",
                    job == null ? null : job.workflowRunId(),
                    job == null ? null : job.sessionId(),
                    job == null ? null : job.tenantId(),
                    attemptNo,
                    e.toString());
            return scheduleRetry(job, attemptNo, "EXCEPTION", safeMsg(e.getMessage()), latencyMs);
        }
    }

    private HandleResult failPermanently(WorkflowDispatchJob job, int attemptNo, String errorCode, String detail, long startNs) {
        long latencyMs = (System.nanoTime() - startNs) / 1_000_000L;
        publishOutcome(WorkflowOutcome.from(job, attemptNo, "FAILED_PERMANENT", errorCode, detail, latencyMs, null));
        publishDlq(job, attemptNo, errorCode);
        publishFailedResult(job, errorCode, detail);
        return HandleResult.ack();
    }

    private static String providerType(WorkflowDispatchJob job) {
        return LlmRouter.normalizeType(job.provider() == null ? null : job.provider().type());
    }

    /** Results report the provider that actually ran (blank types default to bedrock); params are omitted. */
    private static WorkflowDispatchJob.Provider resultProvider(WorkflowDispatchJob job) {
        return job.provider() == null ? null : new WorkflowDispatchJob.Provider(providerType(job), job.provider().modelId(), null);
    }

    private void validate(WorkflowDispatchJob job) throws PermanentFailureException {
        if (job == null) throw new PermanentFailureException("INVALID_JOB", "job is null");
        if (isBlank(job.workflowRunId())) throw new PermanentFailureException("INVALID_JOB", "workflow_run_id is missing");
        if (isBlank(job.sessionId())) throw new PermanentFailureException("INVALID_JOB", "session_id is missing");
        if (isBlank(job.tenantId())) throw new PermanentFailureException("INVALID_JOB", "tenant_id is missing");
        if (job.trigger() == null || isBlank(job.trigger().type())) {
            throw new PermanentFailureException("INVALID_JOB", "trigger.type is missing");
        }
        if (job.trigger().type().startsWith("transcript.") && isBlank(job.trigger().trackId())) {
            throw new PermanentFailureException("INVALID_JOB", "trigger.track_id is missing");
        }
        if (job.provider() == null || isBlank(job.provider().modelId())) throw new PermanentFailureException("INVALID_JOB", "provider.model_id is missing");
        if (job.prompt() == null || isBlank(job.prompt().text())) throw new PermanentFailureException("INVALID_JOB", "prompt.text is missing");
    }

    private HandleResult scheduleRetry(WorkflowDispatchJob job, int attemptNo, String errorCode, String detail, long latencyMs) {
        long now = System.currentTimeMillis();
        RetryPlanner.Next next = retryPlanner.next(attemptNo, now);
        if (next.decision() == RetryPlanner.Decision.DLQ) {
            publishOutcome(WorkflowOutcome.from(job, attemptNo, "FAILED_TRANSIENT", errorCode, detail, latencyMs, null));
            publishDlq(job, attemptNo, errorCode);
            publishFailedResult(job, errorCode, detail);
            return HandleResult.ack();
        }

        String retryTopic = retryTopicForAttempt(attemptNo + 1);
        publishOutcome(WorkflowOutcome.from(job, attemptNo, "RETRYING", errorCode, detail, latencyMs, retryTopic));

        WorkflowDispatchJob nextJob = new WorkflowDispatchJob(
                job.schemaVersion(),
                job.workflowRunId(),
                job.tenantId(),
                job.sessionId(),
                job.workflowId(),
                job.trigger(),
                job.provider(),
                job.prompt(),
                job.input(),
                job.stream(),
                new WorkflowDispatchJob.Attempt(attemptNo + 1)
        );
        try {
            byte[] payload = objectMapper.writeValueAsBytes(nextJob);
            ProducerRecord<String, byte[]> rec = new ProducerRecord<>(retryTopic, job.sessionId(), payload);
            next.notBeforeEpochMsOpt().ifPresent(v -> rec.headers().add(RetryPlanner.HDR_NOT_BEFORE_EPOCH_MS, Long.toString(v).getBytes(StandardCharsets.UTF_8)));
            logSend(kafka.send(rec), "workflow_dispatch_retry", retryTopic);
        } catch (Exception e) {
            // If we can't publish retry, DLQ it.
            publishDlq(job, attemptNo, "RETRY_PUBLISH_FAILED");
            publishFailedResult(job, "RETRY_PUBLISH_FAILED", safeMsg(e.getMessage()));
        }
        return HandleResult.ack();
    }

    private String retryTopicForAttempt(int attemptNo) {
        WorkflowRunnerProperties.Kafka.Topics t = props.kafka().topics();
        return switch (attemptNo) {
            case 1 -> t.workflowDispatchRetry10s();
            case 2 -> t.workflowDispatchRetry2m();
            case 3 -> t.workflowDispatchRetry10m();
            default -> t.workflowDispatchRetry2h();
        };
    }

    private void publishDlq(WorkflowDispatchJob job, int attemptNo, String reason) {
        if (job == null) return;
        try {
            byte[] payload = objectMapper.writeValueAsBytes(job);
            ProducerRecord<String, byte[]> rec = new ProducerRecord<>(props.kafka().topics().workflowDispatchDlq(), job.sessionId(), payload);
            rec.headers().add("X-Nanosamurai-Dlq-Reason", reason.getBytes(StandardCharsets.UTF_8));
            rec.headers().add("X-Nanosamurai-Attempt-No", Integer.toString(attemptNo).getBytes(StandardCharsets.UTF_8));
            logSend(kafka.send(rec), "workflow_dispatch_dlq", props.kafka().topics().workflowDispatchDlq());
        } catch (Exception e) {
            log.error("Failed to publish DLQ message (dropping): {}", e.toString());
        }
    }

    private void publishOutcome(WorkflowOutcome outcome) {
        try {
            String key = outcome.sessionId() == null ? "" : outcome.sessionId();
            byte[] payload = objectMapper.writeValueAsBytes(outcome);
            logSend(kafka.send(props.kafka().topics().workflowOutcome(), key, payload), "workflow_outcome", props.kafka().topics().workflowOutcome());
        } catch (Exception e) {
            log.error("Failed to publish workflow outcome (dropping): {}", e.toString());
        }
    }

    private void publishResult(WorkflowResult result) {
        try {
            String key = result.sessionId() == null ? "" : result.sessionId();
            byte[] payload = objectMapper.writeValueAsBytes(result);
            logSend(kafka.send(props.kafka().topics().workflowResult(), key, payload), "workflow_result", props.kafka().topics().workflowResult());
        } catch (Exception e) {
            log.error("Failed to publish workflow result (dropping): {}", e.toString());
        }
    }

    private void logSend(CompletableFuture<SendResult<String, byte[]>> fut,
                         String messageType,
                         String topic) {
        // Kafka send completes asynchronously and may run outside the MDC scope set by the listener.
        // Capture MDC now and restore it in the completion callback so publish logs retain trace_id.
        Map<String, String> mdc = MDC.getCopyOfContextMap();
        fut.whenComplete((result, ex) -> {
            if (mdc != null) {
                MDC.setContextMap(mdc);
            } else {
                MDC.clear();
            }
            try {
                if (ex != null) {
                    // Failures should be warn so they don't get missed.
                    log.warn("kafka_publish_failed message_type={} topic={} err={}", messageType, topic, ex.toString());
                    return;
                }

                if (result == null || result.getRecordMetadata() == null) {
                    log.info("kafka_published message_type={} topic={}", messageType, topic);
                    return;
                }
                var md = result.getRecordMetadata();
                log.info("kafka_published message_type={} topic={} partition={} offset={}",
                        messageType, md.topic(), md.partition(), md.offset());
            } finally {
                MDC.clear();
            }
        });
    }

    private void publishFailedResult(WorkflowDispatchJob job, String errorCode, String errorDetail) {
        if (job == null) return;
        publishResult(new WorkflowResult(
                1,
                job.workflowRunId(),
                job.tenantId(),
                job.sessionId(),
                job.workflowId(),
                job.trigger(),
                "failed",
                new WorkflowResult.Render(null, null),
                resultProvider(job),
                null,
                Instant.now(),
                job.stream(),
                errorCode,
                safeMsg(errorDetail)
        ));
    }

    private static String buildUserPrompt(WorkflowDispatchJob job) {
        StringBuilder sb = new StringBuilder();
        sb.append("Workflow prompt:\n");
        sb.append(job.prompt() == null ? "" : job.prompt().text());
        sb.append("\n\n");

        var transcript = job.input() == null ? null : job.input().transcript();
        if (transcript != null && transcript.segments() != null) {
            sb.append("Transcript segments (most recent tail):\n");
            for (WorkflowDispatchJob.Input.Transcript.Segment seg : transcript.segments()) {
                if (seg == null) continue;
                String speaker = seg.speaker() == null ? "" : seg.speaker();
                String text = seg.text() == null ? "" : seg.text();
                sb.append("- ").append(speaker).append(": ").append(text).append("\n");
            }
        }

        return sb.toString();
    }

    private static boolean isBlank(String s) {
        return s == null || s.isBlank();
    }

    private static String safeMsg(String s) {
        if (s == null) return null;
        String trimmed = s.replaceAll("[\\r\\n\\t]", " ");
        if (trimmed.length() > 200) {
            return trimmed.substring(0, 200);
        }
        return trimmed;
    }

    private static class PermanentFailureException extends Exception {
        private final String errorCode;

        PermanentFailureException(String errorCode, String msg) {
            super(msg);
            this.errorCode = errorCode;
        }

        public String errorCode() {
            return errorCode;
        }
    }

    public record HandleResult(boolean shouldAck) {
        public static HandleResult ack() {
            return new HandleResult(true);
        }
    }
}
