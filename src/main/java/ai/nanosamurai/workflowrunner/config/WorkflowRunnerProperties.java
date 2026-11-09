package ai.nanosamurai.workflowrunner.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.util.List;

@ConfigurationProperties(prefix = "workflow-runner")
public record WorkflowRunnerProperties(
        Kafka kafka,
        Retry retry,
        Llm llm
) {
    public record Kafka(
            String bootstrapServers,
            String consumerGroupId,
            String securityProtocol,
            Topics topics
    ) {
        public record Topics(
                String workflowDispatch,
                String workflowDispatchRetry10s,
                String workflowDispatchRetry2m,
                String workflowDispatchRetry10m,
                String workflowDispatchRetry2h,
                String workflowDispatchDlq,
                String workflowResult,
                String workflowOutcome
        ) {
        }
    }

    public record Retry(
            int maxAttempts,
            long jitterMs
    ) {
    }

    /**
     * LLM provider settings. Provider endpoints come only from here, never from a workflow job.
     *
     * @param mode      {@code stub} replaces every enabled provider with the offline stub
     * @param providers enabled provider types ({@code bedrock}, {@code openai_compatible})
     */
    public record Llm(
            String mode,
            List<String> providers,
            OpenAi openai
    ) {
        public record OpenAi(
                String baseUrl,
                List<String> models,
                String apiKey,
                int timeoutS
        ) {
            @Override
            public String toString() {
                // Never print the API key.
                return "OpenAi[baseUrl=" + baseUrl + ", models=" + models
                       + ", apiKey=" + (apiKey == null || apiKey.isBlank() ? "<unset>" : "<redacted>")
                       + ", timeoutS=" + timeoutS + "]";
            }
        }
    }
}
