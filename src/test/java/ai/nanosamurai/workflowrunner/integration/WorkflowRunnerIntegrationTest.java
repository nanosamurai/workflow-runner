package ai.nanosamurai.workflowrunner.integration;

import ai.nanosamurai.workflowrunner.WorkflowRunnerApplication;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.net.httpserver.HttpServer;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.test.system.CapturedOutput;
import org.springframework.boot.test.system.OutputCaptureExtension;
import org.springframework.context.ConfigurableApplicationContext;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.EnabledIfDockerAvailable;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Runs the real application wiring against Kafka and a fake OpenAI-compatible server on 127.0.0.1.
 * Only openai_compatible is enabled, so no AWS client is created.
 */
@Tag("integration")
@Testcontainers(disabledWithoutDocker = true)
@EnabledIfDockerAvailable
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class WorkflowRunnerIntegrationTest {

    @Container
    static final KafkaContainer kafka = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.6.1"));

    static {
        kafka.setPortBindings(List.of("127.0.0.1::9093"));
    }

    private static final String RESPONSE_SECRET = "RESPONSE_SECRET_SHOULD_NOT_APPEAR_IN_LOGS_" + UUID.randomUUID();
    private static final AtomicInteger openAiRequests = new AtomicInteger();
    private static final AtomicReference<String> openAiRequest = new AtomicReference<>();
    private static ConfigurableApplicationContext app;
    private static HttpServer fakeOpenAi;

    @AfterAll
    static void shutdown() {
        if (app != null) {
            app.close();
        }
        if (fakeOpenAi != null) {
            fakeOpenAi.stop(0);
        }
    }

    // Runs first: the app starts here, so its console logger writes into this test's captured output.
    @Order(1)
    @ExtendWith(OutputCaptureExtension.class)
    @Test
    void consumesDispatchAndEmitsResultAndOutcome(CapturedOutput output) throws Exception {
        startApp();

        UUID workflowRunId = UUID.randomUUID();
        // Trace id = session id without dashes.
        String sessionId = "s-" + UUID.randomUUID();
        String traceId = sessionId.replace("-", "");
        // Make sure we never leak prompt/transcript bodies in logs.
        String promptSecret = "PROMPT_SECRET_SHOULD_NOT_APPEAR_IN_LOGS_" + UUID.randomUUID();
        String transcriptSecret = "TRANSCRIPT_SECRET_SHOULD_NOT_APPEAR_IN_LOGS_" + UUID.randomUUID();

        sendDispatch(sessionId, """
                {
                  "schema_version": 1,
                  "workflow_run_id": "%s",
                  "tenant_id": "t1",
                  "session_id": "%s",
                  "workflow_id": "w1",
                  "trigger": {"type": "transcript.refined.segment", "track_id": "parakeet", "source_event_id": "e1", "created_at": "%s"},
                  "provider": {"type": "openai_compatible", "model_id": "llama3.1:8b", "params": {"temperature": 0.2, "model": "other"}},
                  "prompt": {"text": "%s"},
                  "input": {"transcript": {"kind": "refined.consolidated", "segments": [{"start_s":1.0,"end_s":2.0,"speaker":"SPEAKER_00","text":"%s"}] }},
                  "stream": {"source_uri": "wss://bff/ws/sessions/%s", "source_node_id": "n1"},
                  "attempt": {"no": 0}
                }
                """.formatted(workflowRunId, sessionId, Instant.now().toString(), promptSecret, transcriptSecret, sessionId));

        String[] got = awaitResultAndOutcome(workflowRunId);
        String resultJson = got[0];
        assertThat(resultJson).isNotNull();
        assertThat(resultJson).contains("\"status\":\"ok\"");
        assertThat(resultJson).contains("\"track_id\":\"parakeet\"");
        assertThat(resultJson).contains("\"markdown\":\"# Summary");
        assertThat(resultJson).contains("\"provider\":{\"type\":\"openai_compatible\",\"model_id\":\"llama3.1:8b\"}");
        assertThat(resultJson).contains("\"usage\":{\"input_tokens\":10,\"output_tokens\":5}");

        String outcomeJson = got[1];
        assertThat(outcomeJson).isNotNull();
        assertThat(outcomeJson).contains("\"status\":\"SUCCESS\"");
        assertThat(outcomeJson).contains("\"track_id\":\"parakeet\"");

        // The model received the prompt and transcript; params cannot swap the model.
        assertThat(openAiRequest.get()).contains(promptSecret).contains(transcriptSecret)
                .contains("\"model\":\"llama3.1:8b\"").doesNotContain("\"model\":\"other\"");

        // Let async publish callbacks flush.
        Thread.sleep(500);

        String logs = output.getAll();
        // We should see trace id + lifecycle events.
        assertThat(logs).contains("trace_id=" + traceId);
        assertThat(logs).contains("workflow_dispatch_received");
        assertThat(logs).contains("workflow_execution_started");
        assertThat(logs).contains("llm_response_received");
        assertThat(logs).contains("kafka_published message_type=workflow_result");
        assertThat(logs).contains("kafka_published message_type=workflow_outcome");
        // Secrets must not appear.
        assertThat(logs).doesNotContain(promptSecret);
        assertThat(logs).doesNotContain(transcriptSecret);
        assertThat(logs).doesNotContain(RESPONSE_SECRET);
    }

    @Order(2)
    @Test
    void failsJobsForDisabledProvidersWithoutCallingAModel() throws Exception {
        startApp();
        UUID workflowRunId = UUID.randomUUID();
        String sessionId = "s-" + UUID.randomUUID();
        int requestsBefore = openAiRequests.get();

        sendDispatch(sessionId, """
                {"schema_version":1,"workflow_run_id":"%s","tenant_id":"t1","session_id":"%s","workflow_id":"w1",
                 "trigger":{"type":"recording.finished"},
                 "provider":{"type":"bedrock","model_id":"anthropic.claude-3-5-sonnet"},
                 "prompt":{"text":"Summarize"},"attempt":{"no":0}}
                """.formatted(workflowRunId, sessionId));

        String[] got = awaitResultAndOutcome(workflowRunId);
        assertThat(got[0]).isNotNull().contains("\"status\":\"failed\"").contains("\"error_code\":\"PROVIDER_NOT_ENABLED\"");
        assertThat(got[1]).isNotNull().contains("\"status\":\"FAILED_PERMANENT\"");
        assertThat(openAiRequests.get()).isEqualTo(requestsBefore);
    }

    private static synchronized void startApp() throws Exception {
        if (app != null) {
            return;
        }
        fakeOpenAi = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        fakeOpenAi.createContext("/v1/chat/completions", exchange -> {
            openAiRequests.incrementAndGet();
            openAiRequest.set(new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8));
            String content = "{\"markdown\":\"# Summary\\n\\n- " + RESPONSE_SECRET + "\\n\",\"json\":{\"ok\":true}}";
            byte[] response = ("{\"choices\":[{\"message\":{\"role\":\"assistant\",\"content\":"
                               + new ObjectMapper().writeValueAsString(content)
                               + "}}],\"usage\":{\"prompt_tokens\":10,\"completion_tokens\":5}}").getBytes(StandardCharsets.UTF_8);
            exchange.getResponseHeaders().set("Content-Type", "application/json");
            exchange.sendResponseHeaders(200, response.length);
            try (var body = exchange.getResponseBody()) {
                body.write(response);
            }
        });
        fakeOpenAi.start();

        String bootstrapServers = kafka.getBootstrapServers();
        if (bootstrapServers.startsWith("PLAINTEXT://")) {
            bootstrapServers = bootstrapServers.substring("PLAINTEXT://".length());
        }

        createTopics(bootstrapServers, List.of(
                "it.workflow.dispatch",
                "it.workflow.dispatch.retry.10s",
                "it.workflow.dispatch.retry.2m",
                "it.workflow.dispatch.retry.10m",
                "it.workflow.dispatch.retry.2h",
                "it.workflow.dispatch.dlq",
                "it.workflow.result",
                "it.workflow.outcome"
        ));

        app = new SpringApplication(WorkflowRunnerApplication.class).run(
                "--server.address=127.0.0.1",
                "--management.server.address=127.0.0.1",
                "--workflow-runner.kafka.bootstrap-servers=" + bootstrapServers,
                "--workflow-runner.kafka.consumer-group-id=workflow-runner-it",
                "--workflow-runner.llm.mode=",
                "--workflow-runner.llm.providers=openai_compatible",
                "--workflow-runner.llm.openai.base-url=http://127.0.0.1:" + fakeOpenAi.getAddress().getPort() + "/v1",
                "--workflow-runner.llm.openai.models=llama3.1:8b,mistral:7b",
                "--workflow-runner.kafka.topics.workflow-dispatch=it.workflow.dispatch",
                "--workflow-runner.kafka.topics.workflow-dispatch-retry-10s=it.workflow.dispatch.retry.10s",
                "--workflow-runner.kafka.topics.workflow-dispatch-retry-2m=it.workflow.dispatch.retry.2m",
                "--workflow-runner.kafka.topics.workflow-dispatch-retry-10m=it.workflow.dispatch.retry.10m",
                "--workflow-runner.kafka.topics.workflow-dispatch-retry-2h=it.workflow.dispatch.retry.2h",
                "--workflow-runner.kafka.topics.workflow-dispatch-dlq=it.workflow.dispatch.dlq",
                "--workflow-runner.kafka.topics.workflow-result=it.workflow.result",
                "--workflow-runner.kafka.topics.workflow-outcome=it.workflow.outcome",
                "--server.port=0",
                "--management.server.port=0"
        );
    }

    private static void sendDispatch(String sessionId, String json) throws Exception {
        try (KafkaProducer<String, byte[]> producer = new KafkaProducer<>(producerProps())) {
            producer.send(new ProducerRecord<>("it.workflow.dispatch", sessionId, json.getBytes(StandardCharsets.UTF_8))).get();
        }
    }

    /** Returns {result, first outcome} JSON for one workflow run, or nulls after a timeout. */
    private static String[] awaitResultAndOutcome(UUID workflowRunId) {
        String marker = "\"workflow_run_id\":\"" + workflowRunId + "\"";
        try (KafkaConsumer<String, byte[]> consumer = new KafkaConsumer<>(consumerProps())) {
            consumer.subscribe(List.of("it.workflow.result", "it.workflow.outcome"));
            String result = null;
            String outcome = null;
            long deadline = System.currentTimeMillis() + 20_000;
            while (System.currentTimeMillis() < deadline && (result == null || outcome == null)) {
                for (var r : consumer.poll(Duration.ofMillis(250))) {
                    String json = new String(r.value(), StandardCharsets.UTF_8);
                    if (!json.contains(marker)) {
                        continue;
                    }
                    if ("it.workflow.result".equals(r.topic())) {
                        result = json;
                    } else if (outcome == null) {
                        outcome = json;
                    }
                }
            }
            return new String[]{result, outcome};
        }
    }

    private static Properties producerProps() {
        Properties p = new Properties();
        p.put("bootstrap.servers", kafka.getBootstrapServers());
        p.put("key.serializer", StringSerializer.class.getName());
        p.put("value.serializer", ByteArraySerializer.class.getName());
        p.put("acks", "all");
        p.put("compression.type", "lz4"); // Verify the maintained codec against a real broker.
        return p;
    }

    private static Properties consumerProps() {
        Properties p = new Properties();
        p.put("bootstrap.servers", kafka.getBootstrapServers());
        p.put("group.id", "workflow-runner-it-consumer-" + UUID.randomUUID());
        p.put("auto.offset.reset", "earliest");
        p.put("enable.auto.commit", "false");
        p.put("key.deserializer", StringDeserializer.class.getName());
        p.put("value.deserializer", ByteArrayDeserializer.class.getName());
        p.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "10");
        return p;
    }

    private static void createTopics(String bootstrapServers, List<String> topicNames)
            throws ExecutionException, InterruptedException {
        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);

        try (AdminClient admin = AdminClient.create(props)) {
            for (String topicName : topicNames) {
                try {
                    admin.createTopics(Collections.singletonList(new NewTopic(topicName, 1, (short) 1)))
                            .all()
                            .get();
                } catch (ExecutionException e) {
                    if (e.getCause() instanceof TopicExistsException) {
                        continue;
                    }
                    throw e;
                }
            }
        }
    }
}
