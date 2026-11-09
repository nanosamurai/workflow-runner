package ai.nanosamurai.workflowrunner.render;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.Map;

public class WorkflowOutputParser {
    private final ObjectMapper objectMapper;

    public WorkflowOutputParser(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    public ParsedOutput parse(String modelText) {
        if (modelText == null) {
            return ParsedOutput.rawMarkdown("");
        }

        try {
            Map<?, ?> m = objectMapper.readValue(modelText, Map.class);
            Object markdown = m.get("markdown");
            if (markdown == null) {
                // Not our expected schema; fall back to raw.
                return ParsedOutput.rawMarkdown(modelText);
            }
            Object json = m.get("json");
            return new ParsedOutput(String.valueOf(markdown), json, true);
        } catch (Exception e) {
            return ParsedOutput.rawMarkdown(modelText);
        }
    }

    public record ParsedOutput(
            String markdown,
            Object json,
            boolean wasJson
    ) {
        public static ParsedOutput rawMarkdown(String markdown) {
            return new ParsedOutput(markdown, null, false);
        }
    }
}
