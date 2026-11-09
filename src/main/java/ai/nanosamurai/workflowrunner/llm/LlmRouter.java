package ai.nanosamurai.workflowrunner.llm;

import java.util.Locale;
import java.util.Map;
import java.util.Set;

/**
 * Selects the client for a job's {@code provider.type}; only operator-enabled providers are present.
 */
public final class LlmRouter {
    public static final String BEDROCK = "bedrock";
    public static final String OPENAI_COMPATIBLE = "openai_compatible";
    public static final Set<String> SUPPORTED = Set.of(BEDROCK, OPENAI_COMPATIBLE);

    private final Map<String, LlmClient> clients;

    public LlmRouter(Map<String, LlmClient> clients) {
        this.clients = Map.copyOf(clients);
    }

    /** Missing or blank types mean {@code bedrock}, the only provider before routing existed. */
    public static String normalizeType(String type) {
        return type == null || type.isBlank() ? BEDROCK : type.trim().toLowerCase(Locale.ROOT);
    }

    public LlmClient resolve(String type) throws LlmException {
        LlmClient client = clients.get(normalizeType(type));
        if (client == null) {
            throw LlmException.permanent("PROVIDER_NOT_ENABLED", "provider type is not enabled on this runner");
        }
        return client;
    }
}
