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

import static org.junit.jupiter.api.Assertions.assertEquals;
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
}
