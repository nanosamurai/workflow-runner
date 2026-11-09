package ai.nanosamurai.workflowrunner.openai;

import ai.nanosamurai.workflowrunner.config.WorkflowRunnerProperties;
import ai.nanosamurai.workflowrunner.llm.LlmClient;
import ai.nanosamurai.workflowrunner.llm.LlmException;
import ai.nanosamurai.workflowrunner.llm.LlmRequest;
import ai.nanosamurai.workflowrunner.llm.LlmResponse;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.net.http.HttpTimeoutException;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

/**
 * Chat completions client for OpenAI-compatible servers (Ollama, vLLM, llama.cpp server).
 *
 * Security:
 * - The endpoint and model allow-list come only from operator configuration, never from a job (SSRF).
 * - Redirects are not followed, no proxy is used and the response body read is capped.
 * - Prompts, transcripts, response bodies and the API key are never logged or echoed in errors.
 */
public final class OpenAiCompatibleClient implements LlmClient {
    private static final Logger log = LoggerFactory.getLogger(OpenAiCompatibleClient.class);

    /** Job params passed through; everything else (including model, messages and stream) is dropped. */
    static final Set<String> ALLOWED_PARAMS = Set.of("temperature", "top_p", "max_tokens", "seed", "stop");
    static final int MAX_RESPONSE_BYTES = 4 * 1024 * 1024;
    private static final Duration MAX_CONNECT_TIMEOUT = Duration.ofSeconds(10);

    private final HttpClient http;
    private final URI endpoint;
    private final Set<String> models;
    private final String apiKey;
    private final Duration timeout;
    private final ObjectMapper objectMapper;

    OpenAiCompatibleClient(URI endpoint, Set<String> models, String apiKey, Duration timeout, ObjectMapper objectMapper) {
        this.endpoint = endpoint;
        this.models = Set.copyOf(models);
        this.apiKey = apiKey == null || apiKey.isBlank() ? null : apiKey.trim();
        this.timeout = timeout;
        this.objectMapper = objectMapper;
        this.http = HttpClient.newBuilder()
                .version(HttpClient.Version.HTTP_1_1)
                .followRedirects(HttpClient.Redirect.NEVER)
                .connectTimeout(timeout.compareTo(MAX_CONNECT_TIMEOUT) < 0 ? timeout : MAX_CONNECT_TIMEOUT)
                .build();
    }

    /** Validates operator configuration and fails fast with a message naming the env var to fix. */
    public static OpenAiCompatibleClient fromConfig(WorkflowRunnerProperties.Llm.OpenAi cfg, ObjectMapper objectMapper) {
        String baseUrl = cfg == null || cfg.baseUrl() == null ? "" : cfg.baseUrl().trim();
        if (baseUrl.isEmpty()) {
            throw new IllegalStateException("WORKFLOW_RUNNER_OPENAI_BASE_URL is required when openai_compatible is enabled");
        }
        URI base;
        try {
            base = URI.create(baseUrl.replaceAll("/+$", ""));
        } catch (IllegalArgumentException e) {
            throw new IllegalStateException("WORKFLOW_RUNNER_OPENAI_BASE_URL is not a valid URL");
        }
        String scheme = base.getScheme() == null ? "" : base.getScheme().toLowerCase(Locale.ROOT);
        if (!(scheme.equals("http") || scheme.equals("https")) || base.getHost() == null
            || base.getRawUserInfo() != null || base.getRawQuery() != null || base.getRawFragment() != null) {
            // Don't echo the value: it may contain credentials.
            throw new IllegalStateException("WORKFLOW_RUNNER_OPENAI_BASE_URL must be an absolute http or https URL "
                                            + "without credentials, query or fragment (e.g. http://ollama:11434/v1)");
        }

        Set<String> models = new LinkedHashSet<>();
        for (String m : cfg.models() == null ? List.<String>of() : cfg.models()) {
            if (m != null && !m.isBlank()) models.add(m.trim());
        }
        if (models.isEmpty()) {
            throw new IllegalStateException("WORKFLOW_RUNNER_OPENAI_MODELS must list at least one model when openai_compatible is enabled");
        }
        if (cfg.timeoutS() <= 0) {
            throw new IllegalStateException("WORKFLOW_RUNNER_OPENAI_TIMEOUT_S must be a positive number of seconds");
        }

        URI endpoint = URI.create(base + "/chat/completions");
        boolean hasKey = cfg.apiKey() != null && !cfg.apiKey().isBlank();
        if (hasKey && scheme.equals("http")) {
            log.warn("openai_compatible API key is sent over plain HTTP; use https unless the endpoint is on a trusted internal network");
        }
        log.info("openai_compatible_enabled endpoint={} models={} api_key_set={} timeout_s={}", endpoint, models, hasKey, cfg.timeoutS());
        return new OpenAiCompatibleClient(endpoint, models, cfg.apiKey(), Duration.ofSeconds(cfg.timeoutS()), objectMapper);
    }

    @Override
    public LlmResponse invoke(LlmRequest request) throws LlmException {
        if (request.modelId() == null || !models.contains(request.modelId())) {
            throw LlmException.permanent("MODEL_NOT_ALLOWED", "model is not in the openai_compatible allow-list");
        }

        byte[] body;
        try {
            body = objectMapper.writeValueAsBytes(requestBody(request));
        } catch (IOException e) {
            throw LlmException.permanent("INVALID_JOB", "request could not be serialized");
        }
        HttpRequest.Builder req = HttpRequest.newBuilder(endpoint)
                .timeout(timeout)
                .header("Content-Type", "application/json")
                .header("Accept", "application/json")
                .POST(HttpRequest.BodyPublishers.ofByteArray(body));
        if (apiKey != null) {
            req.header("Authorization", "Bearer " + apiKey);
        }

        HttpResponse<InputStream> resp;
        try {
            resp = http.send(req.build(), HttpResponse.BodyHandlers.ofInputStream());
        } catch (HttpTimeoutException e) {
            throw LlmException.transientFailure("PROVIDER_TIMEOUT", "openai_compatible request timed out");
        } catch (IOException e) {
            throw LlmException.transientFailure("PROVIDER_UNAVAILABLE", "openai_compatible request failed: " + e.getClass().getSimpleName());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw LlmException.transientFailure("PROVIDER_UNAVAILABLE", "openai_compatible request interrupted");
        }

        int status = resp.statusCode();
        byte[] bytes;
        try (InputStream in = resp.body()) {
            if (status == 408 || status == 429 || status >= 500) {
                throw LlmException.transientFailure("PROVIDER_UNAVAILABLE", "openai_compatible returned HTTP " + status);
            }
            if (status < 200 || status >= 300) {
                // Includes 3xx: redirects are never followed. The body is not echoed (it may quote input).
                throw LlmException.permanent("PROVIDER_REJECTED", "openai_compatible returned HTTP " + status);
            }
            bytes = in.readNBytes(MAX_RESPONSE_BYTES + 1);
        } catch (HttpTimeoutException e) {
            throw LlmException.transientFailure("PROVIDER_TIMEOUT", "openai_compatible response timed out");
        } catch (IOException e) {
            throw LlmException.transientFailure("PROVIDER_UNAVAILABLE", "openai_compatible response failed: " + e.getClass().getSimpleName());
        }
        if (bytes.length > MAX_RESPONSE_BYTES) {
            throw LlmException.permanent("PROVIDER_BAD_RESPONSE", "openai_compatible response exceeds " + MAX_RESPONSE_BYTES + " bytes");
        }
        return parse(bytes);
    }

    Map<String, Object> requestBody(LlmRequest request) {
        Map<String, Object> body = new LinkedHashMap<>();
        if (request.params() instanceof Map<?, ?> params) {
            for (var e : params.entrySet()) {
                if (e.getKey() != null && e.getValue() != null && ALLOWED_PARAMS.contains(String.valueOf(e.getKey()))) {
                    body.put(String.valueOf(e.getKey()), e.getValue());
                }
            }
        }
        // Set after params so a workflow definition cannot override them.
        body.put("model", request.modelId());
        body.put("messages", List.of(
                Map.of("role", "system", "content", request.systemPrompt() == null ? "" : request.systemPrompt()),
                Map.of("role", "user", "content", request.userPrompt() == null ? "" : request.userPrompt())
        ));
        body.put("stream", false);
        return body;
    }

    LlmResponse parse(byte[] bytes) throws LlmException {
        JsonNode root;
        try {
            root = objectMapper.readTree(bytes);
        } catch (IOException e) {
            throw LlmException.permanent("PROVIDER_BAD_RESPONSE", "openai_compatible response is not JSON");
        }
        JsonNode choice = root == null ? null : root.path("choices").path(0);
        JsonNode content = choice == null ? null : choice.path("message").path("content");
        if (content == null || !content.isTextual()) {
            throw LlmException.permanent("PROVIDER_BAD_RESPONSE", "openai_compatible response has no choices[0].message.content");
        }
        if (content.asText().isBlank()) {
            // Reasoning models put their thinking in a separate field; when it uses up
            // max_tokens, the reply content is empty. Fail instead of storing an empty result.
            throw LlmException.permanent("PROVIDER_BAD_RESPONSE", emptyReplyDetail(choice.path("finish_reason")));
        }
        JsonNode usage = root.path("usage");
        return new LlmResponse(content.asText(), intOrNull(usage.path("prompt_tokens")), intOrNull(usage.path("completion_tokens")));
    }

    /**
     * Error detail for an empty reply. Includes the provider's finish_reason only when it is a
     * short lower-case token, so provider text never reaches results.
     */
    static String emptyReplyDetail(JsonNode finishReason) {
        String reason = finishReason != null && finishReason.isTextual() ? finishReason.asText() : "";
        if ("length".equals(reason)) {
            return "openai_compatible reply is empty: the model reached max_tokens before answering "
                    + "(finish_reason=length); raise max_tokens or use a model that reasons less";
        }
        if (reason.matches("[a-z_]{1,32}")) {
            return "openai_compatible reply is empty (finish_reason=" + reason + ")";
        }
        return "openai_compatible reply is empty";
    }

    private static Integer intOrNull(JsonNode n) {
        return n.isIntegralNumber() ? n.intValue() : null;
    }
}
