package ai.nanosamurai.workflowrunner.retry;

import org.apache.kafka.common.header.Headers;

import java.nio.charset.StandardCharsets;
import java.time.Duration;

public class NotBeforeEnforcer {
    public void enforce(Headers headers) throws InterruptedException {
        if (headers == null) return;
        var h = headers.lastHeader(RetryPlanner.HDR_NOT_BEFORE_EPOCH_MS);
        if (h == null) return;
        String s = new String(h.value(), StandardCharsets.UTF_8);
        long notBefore;
        try {
            notBefore = Long.parseLong(s);
        } catch (NumberFormatException e) {
            return;
        }
        long now = System.currentTimeMillis();
        long sleepMs = notBefore - now;
        if (sleepMs <= 0) return;

        // Sleep in chunks so we remain interruptible.
        long remaining = sleepMs;
        while (remaining > 0) {
            long chunk = Math.min(remaining, Duration.ofSeconds(5).toMillis());
            Thread.sleep(chunk);
            remaining -= chunk;
        }
    }
}
