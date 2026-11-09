package ai.nanosamurai.workflowrunner.llm;

public interface LlmClient {
    LlmResponse invoke(LlmRequest request) throws Exception;
}
