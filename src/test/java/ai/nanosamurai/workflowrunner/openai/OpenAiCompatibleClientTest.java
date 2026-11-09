package ai.nanosamurai.workflowrunner.openai;

import ai.nanosamurai.workflowrunner.config.WorkflowRunnerProperties;
import ai.nanosamurai.workflowrunner.llm.LlmException;
import ai.nanosamurai.workflowrunner.llm.LlmRequest;
import ai.nanosamurai.workflowrunner.llm.LlmResponse;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class OpenAiCompatibleClientTest {
    private static final String OK_BODY = """
            {"id":"x","choices":[{"index":0,"message":{"role":"assistant","content":"{\\"markdown\\":\\"# Hi\\"}"}}],
             "usage":{"prompt_tokens":12,"completion_tokens":5,"total_tokens":17}}
            """;

    private final ObjectMapper mapper = new ObjectMapper();
    private final AtomicInteger calls = new AtomicInteger();
    private final AtomicReference<String> path = new AtomicReference<>();
    private final AtomicReference<String> auth = new AtomicReference<>();
    private final AtomicReference<String> body = new AtomicReference<>();
    private HttpServer server;

    @AfterEach
    void stop() {
        if (server != null) server.stop(0);
    }

    @Test
    void postsChatCompletionWithFilteredParamsAndParsesTheReply() throws Exception {
        var client = client(respond(200, OK_BODY), null);
        Map<String, Object> params = Map.of(
                "temperature", 0.2, "top_p", 0.9, "max_tokens", 800, "seed", 7, "stop", List.of("END"),
                "model", "attacker-model", "messages", List.of(), "stream", true, "base_url", "http://evil", "tools", List.of());

        LlmResponse resp = client.invoke(new LlmRequest("llama3.1:8b", params, "system prompt", "user prompt"));

        assertThat(resp.text()).isEqualTo("{\"markdown\":\"# Hi\"}");
        assertThat(resp.inputTokens()).isEqualTo(12);
        assertThat(resp.outputTokens()).isEqualTo(5);
        assertThat(path.get()).isEqualTo("/v1/chat/completions");
        assertThat(auth.get()).isNull();
        JsonNode sent = mapper.readTree(body.get());
        assertThat(sent.path("model").asText()).isEqualTo("llama3.1:8b");
        assertThat(sent.path("stream").asBoolean()).isFalse();
        assertThat(sent.path("messages")).hasSize(2);
        assertThat(sent.path("messages").path(0).path("role").asText()).isEqualTo("system");
        assertThat(sent.path("messages").path(0).path("content").asText()).isEqualTo("system prompt");
        assertThat(sent.path("messages").path(1).path("role").asText()).isEqualTo("user");
        assertThat(sent.path("messages").path(1).path("content").asText()).isEqualTo("user prompt");
        assertThat(sent.path("temperature").asDouble()).isEqualTo(0.2);
        assertThat(sent.path("top_p").asDouble()).isEqualTo(0.9);
        assertThat(sent.path("max_tokens").asInt()).isEqualTo(800);
        assertThat(sent.path("seed").asInt()).isEqualTo(7);
        assertThat(sent.path("stop").path(0).asText()).isEqualTo("END");
        assertThat(sent.has("base_url")).isFalse();
        assertThat(sent.has("tools")).isFalse();
    }

    @Test
    void sendsBearerTokenOnlyWhenConfigured() throws Exception {
        client(respond(200, OK_BODY), "test-key").invoke(request("llama3.1:8b"));
        assertThat(auth.get()).isEqualTo("Bearer test-key");
    }

    @Test
    void rejectsModelsOutsideTheAllowListWithoutACall() throws Exception {
        var client = client(respond(200, OK_BODY), null);
        assertThat(errorOf(() -> client.invoke(request("qwen2.5:72b")))).isEqualTo("permanent:MODEL_NOT_ALLOWED");
        assertThat(calls.get()).isZero();
    }

    @ParameterizedTest
    @ValueSource(ints = {408, 429, 500, 502, 503})
    void retriesTransientHttpStatuses(int status) throws Exception {
        var client = client(respond(status, "{\"error\":\"busy\"}"), null);
        assertThat(errorOf(() -> client.invoke(request("llama3.1:8b")))).isEqualTo("transient:PROVIDER_UNAVAILABLE");
    }

    @ParameterizedTest
    @ValueSource(ints = {400, 401, 404, 422})
    void rejectsOtherClientErrorsPermanentlyWithoutEchoingTheBody(int status) throws Exception {
        var client = client(respond(status, "{\"error\":\"SENSITIVE_TRANSCRIPT_ECHO\"}"), null);
        assertThatThrownBy(() -> client.invoke(request("llama3.1:8b")))
                .isInstanceOfSatisfying(LlmException.class, e -> {
                    assertThat(e.permanent()).isTrue();
                    assertThat(e.errorCode()).isEqualTo("PROVIDER_REJECTED");
                    assertThat(e.getMessage()).contains(Integer.toString(status)).doesNotContain("SENSITIVE");
                });
    }

    @Test
    void doesNotFollowRedirects() throws Exception {
        var redirected = new AtomicInteger();
        var client = client(exchange -> {
            if (exchange.getRequestURI().getPath().startsWith("/elsewhere")) {
                redirected.incrementAndGet();
                send(exchange, 200, OK_BODY);
                return;
            }
            exchange.getResponseHeaders().set("Location", "/elsewhere");
            send(exchange, 307, "");
        }, null);
        assertThat(errorOf(() -> client.invoke(request("llama3.1:8b")))).isEqualTo("permanent:PROVIDER_REJECTED");
        assertThat(redirected.get()).isZero();
    }

    @Test
    void classifiesConnectFailuresAndTimeoutsAsTransient() throws Exception {
        var client = client(exchange -> {
            try {
                Thread.sleep(3_000);
            } catch (InterruptedException ignored) {
                Thread.currentThread().interrupt();
            }
            send(exchange, 200, OK_BODY);
        }, null, 1);
        assertThat(errorOf(() -> client.invoke(request("llama3.1:8b")))).isEqualTo("transient:PROVIDER_TIMEOUT");

        server.stop(0);
        assertThat(errorOf(() -> client.invoke(request("llama3.1:8b")))).isEqualTo("transient:PROVIDER_UNAVAILABLE");
        server = null;
    }

    @Test
    void rejectsMalformedAndOversizedResponses() throws Exception {
        assertThat(errorOf(() -> client(respond(200, "<html>proxy</html>"), null).invoke(request("llama3.1:8b"))))
                .isEqualTo("permanent:PROVIDER_BAD_RESPONSE");
        stop();
        assertThat(errorOf(() -> client(respond(200, "{\"choices\":[]}"), null).invoke(request("llama3.1:8b"))))
                .isEqualTo("permanent:PROVIDER_BAD_RESPONSE");
        stop();
        String huge = "{\"pad\":\"" + "x".repeat(OpenAiCompatibleClient.MAX_RESPONSE_BYTES) + "\"}";
        assertThat(errorOf(() -> client(respond(200, huge), null).invoke(request("llama3.1:8b"))))
                .isEqualTo("permanent:PROVIDER_BAD_RESPONSE");
    }

    @Test
    void rejectsEmptyRepliesAndExplainsTheTokenLimit() throws Exception {
        String lengthReply = "{\"choices\":[{\"finish_reason\":\"length\",\"message\":{\"content\":\"\","
                + "\"reasoning\":\"thinking...\"}}],\"usage\":{\"prompt_tokens\":2156,\"completion_tokens\":800}}";
        LlmException e = exceptionOf(() -> client(respond(200, lengthReply), null).invoke(request("llama3.1:8b")));
        assertThat(e.permanent()).isTrue();
        assertThat(e.errorCode()).isEqualTo("PROVIDER_BAD_RESPONSE");
        assertThat(e.getMessage()).contains("finish_reason=length").contains("max_tokens");
        stop();
        String blankStop = "{\"choices\":[{\"finish_reason\":\"stop\",\"message\":{\"content\":\"  \\n\"}}]}";
        e = exceptionOf(() -> client(respond(200, blankStop), null).invoke(request("llama3.1:8b")));
        assertThat(e.errorCode()).isEqualTo("PROVIDER_BAD_RESPONSE");
        assertThat(e.getMessage()).isEqualTo("openai_compatible reply is empty (finish_reason=stop)");
        stop();
        String oddReason = "{\"choices\":[{\"finish_reason\":\"Ignore <b>this</b>\",\"message\":{\"content\":\"\"}}]}";
        e = exceptionOf(() -> client(respond(200, oddReason), null).invoke(request("llama3.1:8b")));
        assertThat(e.getMessage()).isEqualTo("openai_compatible reply is empty");
    }

    @Test
    void missingUsageLeavesTokenCountsNull() throws Exception {
        LlmResponse resp = client(respond(200, "{\"choices\":[{\"message\":{\"content\":\"plain\"}}]}"), null)
                .invoke(request("llama3.1:8b"));
        assertThat(resp.text()).isEqualTo("plain");
        assertThat(resp.inputTokens()).isNull();
        assertThat(resp.outputTokens()).isNull();
    }

    @Test
    void validatesOperatorConfiguration() {
        assertConfigError(cfg(null, List.of("m")), "WORKFLOW_RUNNER_OPENAI_BASE_URL is required");
        assertConfigError(cfg("ftp://ollama:21/v1", List.of("m")), "WORKFLOW_RUNNER_OPENAI_BASE_URL must be");
        assertConfigError(cfg("ollama:11434/v1", List.of("m")), "WORKFLOW_RUNNER_OPENAI_BASE_URL must be");
        assertConfigError(cfg("http://user:secret@ollama:11434/v1", List.of("m")), "WORKFLOW_RUNNER_OPENAI_BASE_URL must be");
        assertConfigError(cfg("http://ollama:11434/v1", List.of(" ")), "WORKFLOW_RUNNER_OPENAI_MODELS");
        assertConfigError(new WorkflowRunnerProperties.Llm.OpenAi("http://ollama:11434/v1", List.of("m"), null, 0),
                "WORKFLOW_RUNNER_OPENAI_TIMEOUT_S");
        assertThatThrownBy(() -> OpenAiCompatibleClient.fromConfig(cfg("http://user:secret@ollama:11434/v1", List.of("m")), mapper))
                .hasMessageNotContaining("secret");
    }

    @Test
    void configToStringRedactsTheApiKey() {
        var cfg = new WorkflowRunnerProperties.Llm.OpenAi("http://ollama:11434/v1", List.of("m"), "super-secret", 300);
        assertThat(cfg.toString()).doesNotContain("super-secret").contains("<redacted>");
    }

    private void assertConfigError(WorkflowRunnerProperties.Llm.OpenAi cfg, String message) {
        assertThatThrownBy(() -> OpenAiCompatibleClient.fromConfig(cfg, mapper))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining(message);
    }

    private static WorkflowRunnerProperties.Llm.OpenAi cfg(String baseUrl, List<String> models) {
        return new WorkflowRunnerProperties.Llm.OpenAi(baseUrl, models, null, 300);
    }

    private static LlmRequest request(String model) {
        return new LlmRequest(model, null, "system", "user");
    }

    private static String errorOf(Invocation invocation) {
        LlmException e = exceptionOf(invocation);
        return (e.permanent() ? "permanent:" : "transient:") + e.errorCode();
    }

    private static LlmException exceptionOf(Invocation invocation) {
        try {
            invocation.run();
        } catch (LlmException e) {
            return e;
        } catch (Exception e) {
            throw new AssertionError(e);
        }
        throw new AssertionError("expected LlmException");
    }

    private interface Invocation {
        void run() throws Exception;
    }

    private interface Handler {
        void handle(HttpExchange exchange) throws IOException;
    }

    private Handler respond(int status, String responseBody) {
        return exchange -> send(exchange, status, responseBody);
    }

    private OpenAiCompatibleClient client(Handler handler, String apiKey) throws IOException {
        return client(handler, apiKey, 5);
    }

    private OpenAiCompatibleClient client(Handler handler, String apiKey, int timeoutS) throws IOException {
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.setExecutor(Executors.newCachedThreadPool());
        server.createContext("/", exchange -> {
            calls.incrementAndGet();
            path.set(exchange.getRequestURI().getPath());
            auth.set(exchange.getRequestHeaders().getFirst("Authorization"));
            body.set(new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8));
            handler.handle(exchange);
        });
        server.start();
        String base = "http://127.0.0.1:" + server.getAddress().getPort() + "/v1/";
        return OpenAiCompatibleClient.fromConfig(
                new WorkflowRunnerProperties.Llm.OpenAi(base, List.of("llama3.1:8b", "mistral:7b"), apiKey, timeoutS), mapper);
    }

    private static void send(HttpExchange exchange, int status, String responseBody) throws IOException {
        byte[] bytes = responseBody.getBytes(StandardCharsets.UTF_8);
        exchange.getResponseHeaders().set("Content-Type", "application/json");
        exchange.sendResponseHeaders(status, bytes.length == 0 ? -1 : bytes.length);
        try (var out = exchange.getResponseBody()) {
            out.write(bytes);
        } catch (IOException ignored) {
            // Client may close early (timeouts, capped reads).
        }
    }
}
