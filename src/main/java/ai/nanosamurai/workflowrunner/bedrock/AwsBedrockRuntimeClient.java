package ai.nanosamurai.workflowrunner.bedrock;

import ai.nanosamurai.workflowrunner.llm.LlmClient;
import ai.nanosamurai.workflowrunner.llm.LlmRequest;
import ai.nanosamurai.workflowrunner.llm.LlmResponse;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.bedrockruntime.BedrockRuntimeClient;
import software.amazon.awssdk.services.bedrockruntime.model.InvokeModelRequest;
import software.amazon.awssdk.services.bedrockruntime.model.InvokeModelResponse;

import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Minimal Bedrock runtime adapter.
 *
 * This uses the generic InvokeModel API and wraps the request into an Anthropic-compatible shape:
 * https://docs.aws.amazon.com/bedrock/latest/userguide/model-parameters-anthropic-claude-messages.html
 *
 * We keep this implementation deliberately small; model-specific tweaks can be added later.
 */
public class AwsBedrockRuntimeClient implements LlmClient {
    private static final Logger log = LoggerFactory.getLogger(AwsBedrockRuntimeClient.class);

    private final BedrockRuntimeClient client;
    private final ObjectMapper objectMapper;

    public AwsBedrockRuntimeClient(BedrockRuntimeClient client, ObjectMapper objectMapper) {
        this.client = client;
        this.objectMapper = objectMapper;
    }

    @Override
    public LlmResponse invoke(LlmRequest request) throws Exception {
        // Anthropic Claude "messages" API payload.
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("anthropic_version", "bedrock-2023-05-31");
        if (request.params() instanceof Map<?, ?> pMap) {
            // Merge params last so users can override defaults.
            for (var e : pMap.entrySet()) {
                if (e.getKey() != null) {
                    body.put(String.valueOf(e.getKey()), e.getValue());
                }
            }
        }

        if (request.systemPrompt() != null && !request.systemPrompt().isBlank()) {
            body.put("system", request.systemPrompt());
        }

        body.put("messages", List.of(
                Map.of(
                        "role", "user",
                        "content", List.of(Map.of("type", "text", "text", request.userPrompt() == null ? "" : request.userPrompt()))
                )
        ));

        byte[] json = objectMapper.writeValueAsBytes(body);

        InvokeModelRequest req = InvokeModelRequest.builder()
                .modelId(request.modelId())
                .contentType("application/json")
                .accept("application/json")
                .body(SdkBytes.fromByteArray(json))
                .build();

        InvokeModelResponse resp = client.invokeModel(req);
        String respBody = resp.body() == null ? null : resp.body().asString(StandardCharsets.UTF_8);

        // Best-effort parse of Anthropic response; fall back to raw JSON/text.
        try {
            Map<?, ?> m = objectMapper.readValue(respBody, Map.class);
            Object content = m.get("content");
            String text = null;
            if (content instanceof List<?> list && !list.isEmpty()) {
                Object first = list.getFirst();
                if (first instanceof Map<?, ?> cm) {
                    Object t = cm.get("text");
                    if (t != null) {
                        text = String.valueOf(t);
                    }
                }
            }

            Integer inputTokens = null;
            Integer outputTokens = null;
            Object usage = m.get("usage");
            if (usage instanceof Map<?, ?> u) {
                Object in = u.get("input_tokens");
                Object out = u.get("output_tokens");
                if (in instanceof Number n) inputTokens = n.intValue();
                if (out instanceof Number n) outputTokens = n.intValue();
            }
            return new LlmResponse(text != null ? text : respBody, inputTokens, outputTokens);
        } catch (Exception parseEx) {
            log.debug("Failed to parse Bedrock response JSON, returning raw text: {}", parseEx.toString());
            return new LlmResponse(respBody, null, null);
        }
    }
}
