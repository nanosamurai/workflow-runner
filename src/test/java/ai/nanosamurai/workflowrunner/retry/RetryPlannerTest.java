package ai.nanosamurai.workflowrunner.retry;

import ai.nanosamurai.workflowrunner.config.WorkflowRunnerProperties;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class RetryPlannerTest {

    @Test
    void plansRetryTiersAndThenDlq() {
        RetryPlanner planner = new RetryPlanner(new WorkflowRunnerProperties.Retry(5, 0));
        long now = 1000L;

        assertThat(planner.next(0, now).decision()).isEqualTo(RetryPlanner.Decision.RETRY);
        assertThat(planner.next(1, now).decision()).isEqualTo(RetryPlanner.Decision.RETRY);
        assertThat(planner.next(2, now).decision()).isEqualTo(RetryPlanner.Decision.RETRY);
        assertThat(planner.next(3, now).decision()).isEqualTo(RetryPlanner.Decision.RETRY);
        // nextAttempt == 5 => DLQ
        assertThat(planner.next(4, now).decision()).isEqualTo(RetryPlanner.Decision.DLQ);
    }
}
