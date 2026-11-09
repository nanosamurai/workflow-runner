package ai.nanosamurai.workflowrunner.logging;

import ai.nanosamurai.workflowrunner.model.WorkflowDispatchJob;
import org.slf4j.MDC;

/**
 * Central place for MDC keys.
 *
 * Important:
 * - Do not put prompt/transcript bodies into MDC.
 * - Keep values small.
 */
public final class LoggingContext {
    private LoggingContext() {
    }

    // Primary trace id (as requested): session UUID with dashes stripped.
    public static final String MDC_TRACE_ID = "trace_id";

    public static final String MDC_WORKFLOW_RUN_ID = "workflow_run_id";
    public static final String MDC_TENANT_ID = "tenant_id";
    public static final String MDC_WORKFLOW_ID = "workflow_id";
    public static final String MDC_ATTEMPT_NO = "attempt_no";

    public static final String MDC_KAFKA_TOPIC = "kafka_topic";
    public static final String MDC_KAFKA_PARTITION = "kafka_partition";
    public static final String MDC_KAFKA_OFFSET = "kafka_offset";

    public static String traceIdFromSessionId(String sessionId) {
        if (sessionId == null) return null;
        // session id is a UUID-ish string, but we want it without dashes.
        return sessionId.replace("-", "");
    }

    public static void putFromJob(WorkflowDispatchJob job) {
        if (job == null) return;
        String traceId = traceIdFromSessionId(job.sessionId());
        if (traceId != null && !traceId.isBlank()) MDC.put(MDC_TRACE_ID, traceId);
        if (job.workflowRunId() != null && !job.workflowRunId().isBlank()) MDC.put(MDC_WORKFLOW_RUN_ID, job.workflowRunId());
        if (job.tenantId() != null && !job.tenantId().isBlank()) MDC.put(MDC_TENANT_ID, job.tenantId());
        if (job.workflowId() != null && !job.workflowId().isBlank()) MDC.put(MDC_WORKFLOW_ID, job.workflowId());
        int attemptNo = job.attempt() == null ? 0 : job.attempt().no();
        MDC.put(MDC_ATTEMPT_NO, Integer.toString(attemptNo));
    }
}
