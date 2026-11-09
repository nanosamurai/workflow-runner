package ai.nanosamurai.workflowrunner.config;

import ai.nanosamurai.workflowrunner.llm.LlmClient;
import ai.nanosamurai.workflowrunner.llm.LlmRequest;
import ai.nanosamurai.workflowrunner.llm.LlmRouter;
import ai.nanosamurai.workflowrunner.llm.LlmResponse;
import ai.nanosamurai.workflowrunner.llm.StubLlmClient;
import ai.nanosamurai.workflowrunner.llm.LlmException;
import ai.nanosamurai.workflowrunner.openai.OpenAiCompatibleClient;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class LlmRouterConfigTest {
    private final ObjectMapper mapper = new ObjectMapper();
    private final AtomicInteger bedrockCreated = new AtomicInteger();
    private final Supplier<LlmClient> bedrockFactory = () -> {
        bedrockCreated.incrementAndGet();
        return request -> new LlmResponse("bedrock", null, null);
    };

    @Test
    void enablesBedrock() throws Exception {
        LlmRouter router = ServiceConfig.buildLlmRouter(llm(null, List.of("bedrock")), mapper, bedrockFactory);

        assertThat(bedrockCreated.get()).isEqualTo(1);
        assertThat(router.resolve(null).invoke(request()).text()).isEqualTo("bedrock");
    }

    @Test
    void rejectsUnknownAndEmptyProviderLists() {
        assertThatThrownBy(() -> ServiceConfig.buildLlmRouter(llm(null, List.of("bedrock", "gemini")), mapper, bedrockFactory))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("WORKFLOW_RUNNER_PROVIDERS")
                .hasMessageContaining("gemini");
        assertThatThrownBy(() -> ServiceConfig.buildLlmRouter(llm(null, List.of(" ")), mapper, bedrockFactory))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("at least one provider");
        assertThat(bedrockCreated.get()).isZero();
    }

    @Test
    void openAiCompatibleOnlyNeverCreatesBedrock() throws Exception {
        var openai = new WorkflowRunnerProperties.Llm.OpenAi("http://127.0.0.1:11434/v1", List.of("llama3.1:8b"), null, 300);
        LlmRouter router = ServiceConfig.buildLlmRouter(
                new WorkflowRunnerProperties.Llm("", List.of("openai_compatible"), openai), mapper, bedrockFactory);

        assertThat(bedrockCreated.get()).isZero();
        assertThat(router.resolve("openai_compatible")).isInstanceOf(OpenAiCompatibleClient.class);
        assertThatThrownBy(() -> router.resolve("bedrock"))
                .isInstanceOfSatisfying(LlmException.class, e -> {
                    assertThat(e.permanent()).isTrue();
                    assertThat(e.errorCode()).isEqualTo("PROVIDER_NOT_ENABLED");
                });
    }

    @Test
    void openAiCompatibleFailsFastWithoutBaseUrl() {
        assertThatThrownBy(() -> ServiceConfig.buildLlmRouter(llm(null, List.of("openai_compatible")), mapper, bedrockFactory))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("WORKFLOW_RUNNER_OPENAI_BASE_URL");
    }

    @Test
    void stubReplacesEnabledProvidersWithoutCreatingBedrock() throws Exception {
        LlmRouter router = ServiceConfig.buildLlmRouter(llm("stub", List.of("bedrock", "openai_compatible")), mapper, bedrockFactory);

        assertThat(bedrockCreated.get()).isZero();
        assertThat(router.resolve("bedrock")).isInstanceOf(StubLlmClient.class);
        assertThat(router.resolve("openai_compatible")).isInstanceOf(StubLlmClient.class);
    }

    static WorkflowRunnerProperties.Llm llm(String mode, List<String> providers) {
        return new WorkflowRunnerProperties.Llm(mode, providers,
                new WorkflowRunnerProperties.Llm.OpenAi(null, List.of(), null, 300));
    }

    static LlmRequest request() {
        return new LlmRequest("model", null, "system", "user");
    }
}
