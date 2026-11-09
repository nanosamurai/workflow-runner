package ai.nanosamurai.workflowrunner.kafka;

import ai.nanosamurai.workflowrunner.model.WorkflowDispatchJob;
import ai.nanosamurai.workflowrunner.logging.LoggingContext;
import ai.nanosamurai.workflowrunner.retry.NotBeforeEnforcer;
import ai.nanosamurai.workflowrunner.service.WorkflowService;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

@Component
public class WorkflowDispatchListeners {
    private static final Logger log = LoggerFactory.getLogger(WorkflowDispatchListeners.class);

    private final ObjectMapper objectMapper;
    private final WorkflowService workflowService;
    private final NotBeforeEnforcer notBeforeEnforcer;

    public WorkflowDispatchListeners(
            ObjectMapper objectMapper,
            WorkflowService workflowService,
            NotBeforeEnforcer notBeforeEnforcer
    ) {
        this.objectMapper = objectMapper;
        this.workflowService = workflowService;
        this.notBeforeEnforcer = notBeforeEnforcer;
    }

    @KafkaListener(topics = "${workflow-runner.kafka.topics.workflow-dispatch}")
    public void onDispatch(ConsumerRecord<String, byte[]> record, Acknowledgment ack) {
        handle(record, ack, false);
    }

    @KafkaListener(topics = "${workflow-runner.kafka.topics.workflow-dispatch-retry-10s}")
    public void onRetry10s(ConsumerRecord<String, byte[]> record, Acknowledgment ack) {
        handle(record, ack, true);
    }

    @KafkaListener(topics = "${workflow-runner.kafka.topics.workflow-dispatch-retry-2m}")
    public void onRetry2m(ConsumerRecord<String, byte[]> record, Acknowledgment ack) {
        handle(record, ack, true);
    }

    @KafkaListener(topics = "${workflow-runner.kafka.topics.workflow-dispatch-retry-10m}")
    public void onRetry10m(ConsumerRecord<String, byte[]> record, Acknowledgment ack) {
        handle(record, ack, true);
    }

    @KafkaListener(topics = "${workflow-runner.kafka.topics.workflow-dispatch-retry-2h}")
    public void onRetry2h(ConsumerRecord<String, byte[]> record, Acknowledgment ack) {
        handle(record, ack, true);
    }

    private void handle(ConsumerRecord<String, byte[]> record, Acknowledgment ack, boolean enforceNotBefore) {
        // We log topic/partition/offset + trace_id for observability.
        // Do not log record.value() because it may contain prompts/transcripts.
        MDC.put(LoggingContext.MDC_KAFKA_TOPIC, record.topic());
        MDC.put(LoggingContext.MDC_KAFKA_PARTITION, Integer.toString(record.partition()));
        MDC.put(LoggingContext.MDC_KAFKA_OFFSET, Long.toString(record.offset()));
        try {
            if (enforceNotBefore) {
                notBeforeEnforcer.enforce(record.headers());
            }

            WorkflowDispatchJob job = objectMapper.readValue(record.value(), WorkflowDispatchJob.class);

            // Set per-job MDC context for all subsequent logs in this processing chain.
            LoggingContext.putFromJob(job);
            log.info("workflow_dispatch_received");
            workflowService.handle(job);
            ack.acknowledge();
            log.info("workflow_dispatch_acked");
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (Exception e) {
            // Don't ack: Kafka will retry.
            log.warn("workflow_dispatch_failed (will reprocess): err={}", e.toString());
        } finally {
            MDC.clear();
        }
    }
}
