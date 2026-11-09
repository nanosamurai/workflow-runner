package ai.nanosamurai.workflowrunner.config;

import ai.nanosamurai.workflowrunner.bedrock.AwsBedrockRuntimeClient;
import ai.nanosamurai.workflowrunner.llm.LlmClient;
import ai.nanosamurai.workflowrunner.llm.LlmRouter;
import ai.nanosamurai.workflowrunner.llm.StubLlmClient;
import ai.nanosamurai.workflowrunner.openai.OpenAiCompatibleClient;
import ai.nanosamurai.workflowrunner.render.WorkflowOutputParser;
import ai.nanosamurai.workflowrunner.retry.RetryPlanner;
import ai.nanosamurai.workflowrunner.service.WorkflowService;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.KafkaTemplate;
import software.amazon.awssdk.services.bedrockruntime.BedrockRuntimeClient;

import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.Supplier;

@Configuration
public class ServiceConfig {

    private static final Logger log = LoggerFactory.getLogger(ServiceConfig.class);

    @Bean
    public WorkflowOutputParser workflowOutputParser(ObjectMapper objectMapper) {
        return new WorkflowOutputParser(objectMapper);
    }

    @Bean
    public LlmRouter llmRouter(WorkflowRunnerProperties props, ObjectMapper objectMapper) {
        // The Bedrock client needs an AWS region (default provider chain), so it is only created when enabled.
        return buildLlmRouter(props.llm(), objectMapper,
                () -> new AwsBedrockRuntimeClient(BedrockRuntimeClient.create(), objectMapper));
    }

    static LlmRouter buildLlmRouter(WorkflowRunnerProperties.Llm llm, ObjectMapper objectMapper, Supplier<LlmClient> bedrockFactory) {
        Set<String> enabled = enabledProviders(llm == null ? null : llm.providers());
        boolean stub = llm != null && llm.mode() != null && llm.mode().trim().equalsIgnoreCase("stub");
        if (stub) {
            log.warn("Using STUB LLM mode for providers {} (no model calls). DO NOT use this in production.", enabled);
        }
        Map<String, LlmClient> clients = new LinkedHashMap<>();
        for (String type : enabled) {
            if (stub) {
                clients.put(type, new StubLlmClient(objectMapper));
            } else if (type.equals(LlmRouter.BEDROCK)) {
                clients.put(type, bedrockFactory.get());
            } else if (type.equals(LlmRouter.OPENAI_COMPATIBLE)) {
                clients.put(type, OpenAiCompatibleClient.fromConfig(llm.openai(), objectMapper));
            }
        }
        log.info("llm_providers_enabled providers={}", enabled);
        return new LlmRouter(clients);
    }

    private static Set<String> enabledProviders(List<String> configured) {
        Set<String> enabled = new LinkedHashSet<>();
        if (configured != null) {
            for (String raw : configured) {
                if (raw == null || raw.isBlank()) continue;
                String type = raw.trim().toLowerCase(Locale.ROOT);
                if (!LlmRouter.SUPPORTED.contains(type)) {
                    throw new IllegalStateException("WORKFLOW_RUNNER_PROVIDERS contains unsupported provider '" + type
                                                    + "'; supported: " + new TreeSet<>(LlmRouter.SUPPORTED));
                }
                enabled.add(type);
            }
        }
        if (enabled.isEmpty()) {
            throw new IllegalStateException("WORKFLOW_RUNNER_PROVIDERS must enable at least one provider; supported: "
                                            + new TreeSet<>(LlmRouter.SUPPORTED));
        }
        return enabled;
    }

    @Bean
    public WorkflowService workflowService(
            WorkflowRunnerProperties props,
            KafkaTemplate<String, byte[]> kafka,
            ObjectMapper objectMapper,
            RetryPlanner retryPlanner,
            LlmRouter llmRouter,
            WorkflowOutputParser outputParser
    ) {
        return new WorkflowService(props, kafka, objectMapper, retryPlanner, llmRouter, outputParser);
    }
}
