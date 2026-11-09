package ai.nanosamurai.workflowrunner.service;

import ai.nanosamurai.workflowrunner.config.WorkflowRunnerProperties;
import ai.nanosamurai.workflowrunner.llm.LlmClient;
import ai.nanosamurai.workflowrunner.llm.LlmException;
import ai.nanosamurai.workflowrunner.llm.LlmResponse;
import ai.nanosamurai.workflowrunner.llm.LlmRouter;
import ai.nanosamurai.workflowrunner.model.WorkflowDispatchJob;
import ai.nanosamurai.workflowrunner.render.WorkflowOutputParser;
import ai.nanosamurai.workflowrunner.retry.RetryPlanner;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

class ProviderRoutingTest {
    private final ObjectMapper mapper = new ObjectMapper().registerModule(new JavaTimeModule());
    private final CapturingKafka kafka = new CapturingKafka();
    private final AtomicInteger bedrockCalls = new AtomicInteger();
    private final AtomicInteger openAiCalls = new AtomicInteger();

    @Test
    void missingProviderTypeRunsOnBedrock() throws Exception {
        service(Map.of(LlmRouter.BEDROCK, counting(bedrockCalls))).handle(job(null, "model"));

        assertThat(bedrockCalls.get()).isEqualTo(1);
        JsonNode result = json(kafka.records.get(1));
        assertThat(kafka.records.get(1).topic()).isEqualTo("result");
        assertThat(result.path("status").asText()).isEqualTo("ok");
        assertThat(result.path("provider").path("type").asText()).isEqualTo("bedrock");
    }

    @Test
    void routesByProviderTypeAndReportsIt() throws Exception {
        service(Map.of(LlmRouter.BEDROCK, counting(bedrockCalls), LlmRouter.OPENAI_COMPATIBLE, counting(openAiCalls)))
                .handle(job(" OpenAI_Compatible ", "llama3.1:8b"));

        assertThat(bedrockCalls.get()).isZero();
        assertThat(openAiCalls.get()).isEqualTo(1);
        assertThat(json(kafka.records.get(1)).path("provider").path("type").asText()).isEqualTo("openai_compatible");
    }

    @Test
    void disabledProviderFailsPermanentlyWithoutACall() throws Exception {
        service(Map.of(LlmRouter.OPENAI_COMPATIBLE, counting(openAiCalls))).handle(job("bedrock", "model"));

        assertThat(openAiCalls.get()).isZero();
        assertPermanentFailure("PROVIDER_NOT_ENABLED", "bedrock");
    }

    @Test
    void clientPermanentFailureGoesToDlq() throws Exception {
        LlmClient rejecting = request -> { throw LlmException.permanent("MODEL_NOT_ALLOWED", "model is not allowed"); };
        service(Map.of(LlmRouter.OPENAI_COMPATIBLE, rejecting)).handle(job("openai_compatible", "other"));

        assertPermanentFailure("MODEL_NOT_ALLOWED", "openai_compatible");
    }

    @Test
    void clientTransientFailureIsRetriedWithItsCode() throws Exception {
        LlmClient unavailable = request -> { throw LlmException.transientFailure("PROVIDER_UNAVAILABLE", "HTTP 503"); };
        service(Map.of(LlmRouter.OPENAI_COMPATIBLE, unavailable)).handle(job("openai_compatible", "m"));

        assertThat(kafka.records).extracting(ProducerRecord::topic).containsExactly("outcome", "retry1");
        JsonNode outcome = json(kafka.records.getFirst());
        assertThat(outcome.path("status").asText()).isEqualTo("RETRYING");
        assertThat(outcome.path("error_code").asText()).isEqualTo("PROVIDER_UNAVAILABLE");
    }

    private void assertPermanentFailure(String errorCode, String providerType) throws Exception {
        assertThat(kafka.records).extracting(ProducerRecord::topic).containsExactly("outcome", "dlq", "result");
        JsonNode outcome = json(kafka.records.get(0));
        assertThat(outcome.path("status").asText()).isEqualTo("FAILED_PERMANENT");
        assertThat(outcome.path("error_code").asText()).isEqualTo(errorCode);
        assertThat(new String(kafka.records.get(1).headers().lastHeader("X-Nanosamurai-Dlq-Reason").value(), StandardCharsets.UTF_8))
                .isEqualTo(errorCode);
        JsonNode result = json(kafka.records.get(2));
        assertThat(result.path("status").asText()).isEqualTo("failed");
        assertThat(result.path("error_code").asText()).isEqualTo(errorCode);
        assertThat(result.path("provider").path("type").asText()).isEqualTo(providerType);
    }

    private LlmClient counting(AtomicInteger calls) {
        return request -> {
            calls.incrementAndGet();
            return new LlmResponse("{\"markdown\":\"done\"}", 1, 2);
        };
    }

    private JsonNode json(ProducerRecord<String, byte[]> record) throws Exception {
        return mapper.readTree(record.value());
    }

    private WorkflowDispatchJob job(String providerType, String model) throws Exception {
        String type = providerType == null ? "" : "\"type\":\"" + providerType + "\",";
        return mapper.readValue("""
                {"schema_version":1,"workflow_run_id":"run","tenant_id":"tenant","session_id":"session","workflow_id":"workflow",
                 "trigger":{"type":"recording.finished"},
                 "provider":{%s"model_id":"%s"},"prompt":{"text":"Summarize"},"attempt":{"no":0}}
                """.formatted(type, model), WorkflowDispatchJob.class);
    }

    private WorkflowService service(Map<String, LlmClient> clients) {
        var props = new WorkflowRunnerProperties(new WorkflowRunnerProperties.Kafka("unused", "unused", "PLAINTEXT",
                new WorkflowRunnerProperties.Kafka.Topics("dispatch", "retry1", "retry2", "retry3", "retry4", "dlq", "result", "outcome")),
                new WorkflowRunnerProperties.Retry(3, 0), null);
        return new WorkflowService(props, kafka, mapper, new RetryPlanner(props.retry()), new LlmRouter(clients), new WorkflowOutputParser(mapper));
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
