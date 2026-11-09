package ai.nanosamurai.workflowrunner;

import ai.nanosamurai.workflowrunner.config.WorkflowRunnerProperties;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;

@SpringBootApplication
@ConfigurationPropertiesScan(basePackageClasses = WorkflowRunnerProperties.class)
public class WorkflowRunnerApplication {

    public static void main(String[] args) {
        SpringApplication.run(WorkflowRunnerApplication.class, args);
    }
}
