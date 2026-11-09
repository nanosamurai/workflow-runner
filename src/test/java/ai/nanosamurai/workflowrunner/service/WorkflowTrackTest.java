package ai.nanosamurai.workflowrunner.service;

import ai.nanosamurai.workflowrunner.llm.LlmClient;
import ai.nanosamurai.workflowrunner.llm.LlmResponse;
import ai.nanosamurai.workflowrunner.llm.LlmRouter;
import ai.nanosamurai.workflowrunner.config.WorkflowRunnerProperties;
import ai.nanosamurai.workflowrunner.model.WorkflowDispatchJob;
import ai.nanosamurai.workflowrunner.render.WorkflowOutputParser;
import ai.nanosamurai.workflowrunner.retry.RetryPlanner;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

class WorkflowTrackTest {
    private final ObjectMapper mapper = new ObjectMapper().registerModule(new JavaTimeModule());
    private final CapturingKafka kafka = new CapturingKafka();

    @Test
    void preservesSelectedTrackInResultAndOutcome() throws Exception {
        var service = service(request -> new LlmResponse("{\"markdown\":\"done\"}", 1, 2));
        for (String track : List.of("whisperx", "parakeet")) {
            service.handle(job(track));
        }
        assertThat(kafka.records).hasSize(4);
        assertTrack(kafka.records.get(0), "outcome", "whisperx");
        assertTrack(kafka.records.get(1), "result", "whisperx");
        assertTrack(kafka.records.get(2), "outcome", "parakeet");
        assertTrack(kafka.records.get(3), "result", "parakeet");
    }

    @Test
    void preservesTrackAcrossRetryAndExhaustedFailure() throws Exception {
        var service = service(request -> { throw new IllegalStateException("provider unavailable"); });
        service.handle(job("parakeet"));
        assertTrack(kafka.records.get(0), "outcome", "parakeet");
        assertTrack(kafka.records.get(1), "retry1", "parakeet");
        var retry = mapper.readValue(kafka.records.get(1).value(), WorkflowDispatchJob.class);
        assertThat(retry.attempt().no()).isEqualTo(1);
        assertThat(retry.workflowRunId()).isEqualTo("run-parakeet");
        service.handle(retry);
        assertTrack(kafka.records.get(2), "outcome", "parakeet");
        assertTrack(kafka.records.get(3), "dlq", "parakeet");
        assertTrack(kafka.records.get(4), "result", "parakeet");
        assertThat(mapper.readTree(kafka.records.get(4).value()).path("status").asText()).isEqualTo("failed");
    }

    @Test
    void rejectsTranscriptWithoutTrackBeforeCallingProvider() throws Exception {
        var calls = new AtomicInteger();
        var service = service(request -> { calls.incrementAndGet(); return new LlmResponse("ok", 0, 0); });
        service.handle(job(""));
        assertThat(calls.get()).isZero();
        assertThat(mapper.readTree(kafka.records.getFirst().value()).path("error_code").asText()).isEqualTo("INVALID_JOB");
    }

    private WorkflowDispatchJob job(String track) throws Exception {
        return mapper.readValue("""
                {"schema_version":1,"workflow_run_id":"run-%s","tenant_id":"tenant","session_id":"session","workflow_id":"workflow-%s",
                 "trigger":{"type":"transcript.refined.segment","track_id":"%s","source_event_id":"event"},
                 "provider":{"type":"bedrock","model_id":"model"},"prompt":{"text":"Summarize"},
                 "input":{"transcript":{"kind":"refined.consolidated","segments":[{"text":"hello"}]}},"attempt":{"no":0}}
                """.formatted(track, track, track), WorkflowDispatchJob.class);
    }

    private WorkflowService service(LlmClient client) {
        var props = new WorkflowRunnerProperties(new WorkflowRunnerProperties.Kafka("unused", "unused", "PLAINTEXT",
                new WorkflowRunnerProperties.Kafka.Topics("dispatch", "retry1", "retry2", "retry3", "retry4", "dlq", "result", "outcome")),
                new WorkflowRunnerProperties.Retry(2, 0), null);
        return new WorkflowService(props, kafka, mapper, new RetryPlanner(props.retry()), new LlmRouter(Map.of("bedrock", client)), new WorkflowOutputParser(mapper));
    }

    private void assertTrack(ProducerRecord<String, byte[]> record, String topic, String track) throws Exception {
        assertThat(record.topic()).isEqualTo(topic);
        assertThat(record.key()).isEqualTo("session");
        assertThat(mapper.readTree(record.value()).path("trigger").path("track_id").asText()).isEqualTo(track);
    }

    private static class CapturingKafka extends KafkaTemplate<String, byte[]> {
        final List<ProducerRecord<String, byte[]>> records = new ArrayList<>();
        CapturingKafka() { super(new DefaultKafkaProducerFactory<>(Map.of())); }
        @Override public CompletableFuture<SendResult<String, byte[]>> send(String topic, String key, byte[] value) {
            return send(new ProducerRecord<>(topic, key, value));
        }
        @Override public CompletableFuture<SendResult<String, byte[]>> send(ProducerRecord<String, byte[]> record) {
            records.add(record);
            return CompletableFuture.completedFuture(null);
        }
    }
}
