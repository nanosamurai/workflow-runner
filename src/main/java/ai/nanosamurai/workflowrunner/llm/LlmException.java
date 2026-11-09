package ai.nanosamurai.workflowrunner.llm;

/**
 * Provider failure with an error code for outcomes and results.
 *
 * Permanent failures go straight to the failed result and DLQ; transient ones use the retry tiers.
 * Messages must stay short and must never contain prompts, transcripts or response bodies.
 */
public class LlmException extends Exception {
    private final String errorCode;
    private final boolean permanent;

    private LlmException(String errorCode, String message, boolean permanent) {
        super(message);
        this.errorCode = errorCode;
        this.permanent = permanent;
    }

    public static LlmException permanent(String errorCode, String message) {
        return new LlmException(errorCode, message, true);
    }

    public static LlmException transientFailure(String errorCode, String message) {
        return new LlmException(errorCode, message, false);
    }

    public String errorCode() {
        return errorCode;
    }

    public boolean permanent() {
        return permanent;
    }
}
