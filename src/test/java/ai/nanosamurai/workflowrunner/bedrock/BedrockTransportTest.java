package ai.nanosamurai.workflowrunner.bedrock;

import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.bedrockruntime.BedrockRuntimeClient;

import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

class BedrockTransportTest {
    @Test
    void synchronousClientLoadsItsTransportAndInvokesALocalEndpoint() throws Exception {
        var receivedBody = new AtomicReference<String>();
        var server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/model/test-model/invoke", exchange -> {
            receivedBody.set(new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8));
            byte[] response = "{\"result\":\"synthetic\"}".getBytes(StandardCharsets.UTF_8);
            exchange.getResponseHeaders().set("Content-Type", "application/json");
            exchange.sendResponseHeaders(200, response.length);
            try (var body = exchange.getResponseBody()) {
                body.write(response);
            }
        });
        server.start();
        try (var client = BedrockRuntimeClient.builder()
                .region(Region.US_EAST_1)
                .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create("test", "test")))
                .endpointOverride(URI.create("http://127.0.0.1:" + server.getAddress().getPort()))
                .overrideConfiguration(c -> c.apiCallTimeout(Duration.ofSeconds(5)))
                .build()) {
            var response = client.invokeModel(r -> r.modelId("test-model")
                    .contentType("application/json").body(SdkBytes.fromUtf8String("{\"prompt\":\"synthetic\"}")));
            assertThat(response.body().asUtf8String()).isEqualTo("{\"result\":\"synthetic\"}");
            assertThat(receivedBody.get()).isEqualTo("{\"prompt\":\"synthetic\"}");
        } finally {
            server.stop(0);
        }
    }
}
