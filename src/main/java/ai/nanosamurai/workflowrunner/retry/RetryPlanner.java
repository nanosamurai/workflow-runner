package ai.nanosamurai.workflowrunner.retry;

import ai.nanosamurai.workflowrunner.config.WorkflowRunnerProperties;

import java.util.Optional;

public class RetryPlanner {
    public static final String HDR_NOT_BEFORE_EPOCH_MS = "X-Nanosamurai-Not-Before-EpochMs";

    private final WorkflowRunnerProperties.Retry retryProps;

    public RetryPlanner(WorkflowRunnerProperties.Retry retryProps) {
        this.retryProps = retryProps;
    }

    public Next next(int attemptNo, long nowEpochMs) {
        int nextAttempt = attemptNo + 1;
        if (nextAttempt >= retryProps.maxAttempts()) {
            return new Next(Decision.DLQ, null);
        }

        long delayMs = switch (nextAttempt) {
            case 1 -> 10_000L;
            case 2 -> 120_000L;
            case 3 -> 600_000L;
            default -> 7_200_000L; // 2h
        };
        long jitter = retryProps.jitterMs() <= 0 ? 0 : (long) (Math.random() * retryProps.jitterMs());
        return new Next(Decision.RETRY, nowEpochMs + delayMs + jitter);
    }

    public enum Decision {
        RETRY,
        DLQ
    }

    public record Next(Decision decision, Long notBeforeEpochMs) {
        public Optional<Long> notBeforeEpochMsOpt() {
            return Optional.ofNullable(notBeforeEpochMs);
        }
    }
}
