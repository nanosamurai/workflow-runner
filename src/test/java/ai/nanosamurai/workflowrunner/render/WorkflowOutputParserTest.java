package ai.nanosamurai.workflowrunner.render;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class WorkflowOutputParserTest {

    @Test
    void parsesJsonWithMarkdown() {
        WorkflowOutputParser p = new WorkflowOutputParser(new ObjectMapper());
        var out = p.parse("{\"markdown\":\"# Hi\",\"json\":{\"a\":1}}");
        assertThat(out.wasJson()).isTrue();
        assertThat(out.markdown()).isEqualTo("# Hi");
        assertThat(out.json()).isInstanceOfAny(java.util.Map.class);
    }

    @Test
    void fallsBackToRawMarkdown() {
        WorkflowOutputParser p = new WorkflowOutputParser(new ObjectMapper());
        var out = p.parse("not json");
        assertThat(out.wasJson()).isFalse();
        assertThat(out.markdown()).isEqualTo("not json");
        assertThat(out.json()).isNull();
    }
}
