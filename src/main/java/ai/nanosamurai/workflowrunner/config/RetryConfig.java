package ai.nanosamurai.workflowrunner.config;

import ai.nanosamurai.workflowrunner.retry.NotBeforeEnforcer;
import ai.nanosamurai.workflowrunner.retry.RetryPlanner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class RetryConfig {

    @Bean
    public RetryPlanner retryPlanner(WorkflowRunnerProperties props) {
        return new RetryPlanner(props.retry());
    }

    @Bean
    public NotBeforeEnforcer notBeforeEnforcer() {
        return new NotBeforeEnforcer();
    }
}
