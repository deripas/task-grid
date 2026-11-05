package io.github.deripas.crawler.urlmanager;

import io.github.deripas.crawler.urlmanager.app.UrlManagerApplication;
import io.github.deripas.crawler.urlmanager.grpc.NodeServer;
import org.junit.jupiter.api.Test;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ConfigurableApplicationContext;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The application starts with its defaults and no blob storage, serves HTTP and the node port.
 * No sidecar is connected, so a fetch has nobody to run it - end to end is UrlManagerIT.
 */
class UrlManagerApplicationTest {
    @Test
    void startsAndRejectsAnInvalidUrl() throws Exception {
        try (ConfigurableApplicationContext context = new SpringApplicationBuilder(UrlManagerApplication.class)
            .run("--server.port=0", "--url-manager.grpc-port=0")) {
            assertTrue(context.getBean(NodeServer.class).port() > 0);
            final int port = context.getEnvironment().getRequiredProperty("local.server.port", Integer.class);

            final HttpRequest request = HttpRequest.newBuilder(URI.create("http://localhost:" + port + "/fetch"))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString("{\"url\": \"ftp://example.com/\"}"))
                .build();
            final HttpResponse<String> response = HttpClient.newHttpClient().send(request, HttpResponse.BodyHandlers.ofString());

            assertEquals(400, response.statusCode(), response.body());
            assertTrue(response.headers().firstValue("Content-Type").orElseThrow().startsWith("application/problem+json"));
        }
    }

    /**
     * In the cluster a result may finish on another node than the one that took the request: after-reply cannot work.
     */
    @Test
    void clusterModeRefusesAckAfterReply() {
        final Exception error = assertThrows(Exception.class, () -> new SpringApplicationBuilder(UrlManagerApplication.class)
            .run("--server.port=0", "--url-manager.grpc-port=0", "--url-manager.queue=cluster",
                "--url-manager.rabbit.enabled=true", "--url-manager.rabbit.ack=after-reply")
            .close());
        assertTrue(rootCause(error).getMessage().contains("after-submit"), String.valueOf(rootCause(error)));
    }

    private static Throwable rootCause(Throwable error) {
        Throwable cause = error;
        while (cause.getCause() != null) {
            cause = cause.getCause();
        }
        return cause;
    }

    /**
     * No sidecar of the fetch kind: the attempt waits for a free slot, and the metric shows it.
     */
    @Test
    void attemptWaitingForAHandlerIsInTheMetrics() throws Exception {
        try (ConfigurableApplicationContext context = new SpringApplicationBuilder(UrlManagerApplication.class)
            .run("--server.port=0", "--url-manager.grpc-port=0")) {
            final int port = context.getEnvironment().getRequiredProperty("local.server.port", Integer.class);
            final HttpClient client = HttpClient.newHttpClient();
            final HttpRequest fetch = HttpRequest.newBuilder(URI.create("http://localhost:" + port + "/fetch"))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString("{\"url\": \"http://example.com/\", \"timeoutMs\": 100}"))
                .build();
            assertEquals(504, client.send(fetch, HttpResponse.BodyHandlers.ofString()).statusCode());

            final HttpRequest scrape = HttpRequest.newBuilder(URI.create("http://localhost:" + port + "/actuator/prometheus")).build();
            final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
            String metrics;
            do {
                Thread.sleep(100);
                metrics = client.send(scrape, HttpResponse.BodyHandlers.ofString()).body();
                // the three gauges of a new kind are registered one after another: wait for the last one
            } while (!metrics.contains("task_grid_worker_capacity{kind=\"fetch\"}") && System.nanoTime() < deadline);

            assertTrue(metrics.contains("task_grid_worker_waiting{kind=\"fetch\"} 1.0"), metrics);
            assertTrue(metrics.contains("task_grid_worker_capacity{kind=\"fetch\"} 0.0"), metrics);
        }
    }
}
