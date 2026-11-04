package io.github.deripas.crawler.urlmanager;

import com.github.tomakehurst.wiremock.WireMockServer;
import com.github.tomakehurst.wiremock.stubbing.ServeEvent;
import io.github.deripas.crawler.fetcher.grpc.FetcherService;
import io.github.deripas.crawler.fetcher.http.ApacheFetcher;
import io.github.deripas.crawler.urlmanager.api.FetchController;
import io.github.deripas.crawler.urlmanager.app.UrlManagerApplication;
import io.github.deripas.crawler.urlmanager.grpc.NodeServer;
import io.grpc.Grpc;
import io.grpc.InsecureServerCredentials;
import io.grpc.Server;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ConfigurableApplicationContext;
import org.testcontainers.Testcontainers;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.images.builder.ImageFromDockerfile;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static com.github.tomakehurst.wiremock.client.WireMock.urlPathMatching;
import static com.github.tomakehurst.wiremock.core.WireMockConfiguration.options;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * End to end: client -> url-manager (this JVM: Spring application, in-memory task-grid node) -> handler-sidecar
 * (the real image) -> fetcher (this JVM) -> site (WireMock); pages above the inline limit of the sidecar go through
 * SeaweedFS. Needs Docker; run by {@code make it}.
 */
class UrlManagerIT {
    private static final Duration WAIT = Duration.ofSeconds(30);
    private static final int S3_PORT = 8333;
    private static final String BUCKET = "results";
    private static final Duration SLOW = Duration.ofMillis(300);
    private static final ImageFromDockerfile SIDECAR_IMAGE = new ImageFromDockerfile("task-grid/handler-sidecar-it", false)
        .withDockerfile(Path.of(System.getProperty("handler-sidecar.dir", "../../handler-sidecar"), "Dockerfile"));
    private static final HttpClient HTTP = HttpClient.newHttpClient();

    private static WireMockServer site;
    private static ExecutorService calls;
    private static ApacheFetcher fetcher;
    private static Server fetcherServer;
    private static Network network;
    private static GenericContainer<?> storage;
    private static ConfigurableApplicationContext urlManager;
    private static GenericContainer<?> sidecar;
    private static int httpPort;

    @BeforeAll
    static void start() throws IOException, InterruptedException {
        site = new WireMockServer(options().dynamicPort());
        site.start();
        calls = Executors.newVirtualThreadPerTaskExecutor();
        fetcher = new ApacheFetcher(Duration.ofSeconds(2), Duration.ofSeconds(10), 100 << 20);
        fetcherServer = Grpc.newServerBuilderForPort(0, InsecureServerCredentials.create())
            .executor(calls)
            .addService(new FetcherService(fetcher))
            .build()
            .start();

        network = Network.newNetwork();
        storage = new GenericContainer<>("chrislusf/seaweedfs:4.48")
            .withCommand("server", "-s3", "-dir=/data", "-master.volumeSizeLimitMB=64", "-volume.max=0")
            .withNetwork(network)
            .withNetworkAliases("seaweedfs")
            .withExposedPorts(S3_PORT)
            .waitingFor(Wait.forListeningPorts(S3_PORT).withStartupTimeout(WAIT));
        storage.start();
        createBucket();

        urlManager = new SpringApplicationBuilder(UrlManagerApplication.class).run(
            "--server.port=0",
            "--url-manager.grpc-port=0",
            "--url-manager.blob.endpoint=" + s3(""),
            "--url-manager.blob.access-key=any",
            "--url-manager.blob.secret-key=any");
        httpPort = urlManager.getEnvironment().getRequiredProperty("local.server.port", Integer.class);
        final int nodePort = urlManager.getBean(NodeServer.class).port();

        Testcontainers.exposeHostPorts(nodePort, fetcherServer.getPort());
        sidecar = new GenericContainer<>(SIDECAR_IMAGE)
            .withNetwork(network)
            .withEnv(Map.of(
                "NODE_ADDRESS", "host.testcontainers.internal:" + nodePort,
                "HANDLER_TARGET", "host.testcontainers.internal:" + fetcherServer.getPort(),
                "HANDLER_METHOD", "/crawler.fetcher.v1.Fetcher/Fetch",
                "WORKER_ID", "fetcher-it",
                "KIND", "fetch",
                "CAPACITY", "4",
                "INLINE_MAX", String.valueOf(64 << 10),
                "BLOB_S3_ENDPOINT", "seaweedfs:" + S3_PORT,
                "BLOB_PREFIX", "it/"))
            .waitingFor(Wait.forLogMessage(".*session open.*", 1).withStartupTimeout(WAIT));
        sidecar.start();
    }

    @AfterAll
    static void stop() throws IOException, InterruptedException {
        sidecar.stop();
        urlManager.close();
        fetcherServer.shutdownNow().awaitTermination(5, TimeUnit.SECONDS);
        calls.shutdownNow();
        fetcher.close();
        site.stop();
        storage.stop();
        network.close();
    }

    @Test
    void pageOfTheSite() throws Exception {
        site.stubFor(get("/page").willReturn(aResponse()
            .withHeader("Content-Type", "text/html")
            .withBody("<html>hello</html>")));

        final HttpResponse<String> response = fetch("http://localhost:" + site.port() + "/page");

        assertEquals(200, response.statusCode(), response.body());
        assertEquals("200", response.headers().firstValue(FetchController.FETCH_STATUS).orElseThrow());
        assertEquals("text/html", response.headers().firstValue("Content-Type").orElseThrow());
        assertEquals("<html>hello</html>", response.body());
    }

    @Test
    void notFoundOfTheSiteIsAPage() throws Exception {
        site.stubFor(get("/missing").willReturn(aResponse().withStatus(404).withBody("no such page")));

        final HttpResponse<String> response = fetch("http://localhost:" + site.port() + "/missing");

        assertEquals(200, response.statusCode(), response.body());
        assertEquals("404", response.headers().firstValue(FetchController.FETCH_STATUS).orElseThrow());
        assertEquals("no such page", response.body());
    }

    @Test
    void unreachableSiteIsBadGateway() throws Exception {
        final HttpResponse<String> response = fetch("http://localhost:1/");

        assertEquals(502, response.statusCode(), response.body());
        assertTrue(response.body().contains("FailedPrecondition"), response.body());
    }

    /**
     * 8 MiB: above the inline limit of the sidecar and the inbound limit of the node - read back from SeaweedFS.
     */
    @Test
    void largePageGoesThroughBlobStorage() throws Exception {
        final String sitemap = sitemap(8 << 20);
        site.stubFor(get("/sitemap.xml").willReturn(aResponse().withHeader("Content-Type", "application/xml").withBody(sitemap)));

        final HttpResponse<String> response = fetch("http://localhost:" + site.port() + "/sitemap.xml");

        assertEquals(200, response.statusCode());
        assertEquals("application/xml", response.headers().firstValue("Content-Type").orElseThrow());
        assertEquals(String.valueOf(sitemap.length()), response.headers().firstValue("Content-Length").orElseThrow());
        assertEquals(sitemap, response.body());
        final HttpRequest list = HttpRequest.newBuilder(s3("/" + BUCKET + "?prefix=it/")).build();
        final String listing = HTTP.send(list, HttpResponse.BodyHandlers.ofString()).body();
        assertTrue(listing.contains("<Key>it/"), listing);
    }

    /**
     * Pages of one host are fetched one at a time; another host does not wait for it ("localhost" and "127.0.0.1"
     * are different hosts of the same WireMock).
     */
    @Test
    void oneFetchOfAHostAtATime() throws Exception {
        site.stubFor(get(urlPathMatching("/slow/.*")).willReturn(aResponse().withFixedDelay((int) SLOW.toMillis()).withBody("slow")));
        final List<String> urls = List.of(
            "http://localhost:" + site.port() + "/slow/1",
            "http://localhost:" + site.port() + "/slow/2",
            "http://localhost:" + site.port() + "/slow/3",
            "http://127.0.0.1:" + site.port() + "/slow/other");

        final List<CompletableFuture<HttpResponse<String>>> responses = urls.stream()
            .map(url -> CompletableFuture.supplyAsync(() -> fetchUnchecked(url), calls))
            .toList();
        for (CompletableFuture<HttpResponse<String>> response : responses) {
            assertEquals(200, response.get(WAIT.toSeconds(), TimeUnit.SECONDS).statusCode());
        }

        final List<ServeEvent> local = served("/slow/", "localhost");
        assertEquals(3, local.size());
        for (int i = 1; i < local.size(); i++) {
            final long gap = started(local.get(i)) - started(local.get(i - 1));
            assertTrue(gap >= SLOW.toMillis() - 10, "fetch " + i + " of localhost started " + gap + " ms after the previous one");
        }
        final long other = started(served("/slow/other", "127.0.0.1").getFirst());
        final long last = started(local.getLast());
        assertTrue(other < last, "the other host waited for localhost");
    }

    private static HttpResponse<String> fetch(String url) throws IOException, InterruptedException {
        final HttpRequest request = HttpRequest.newBuilder(URI.create("http://localhost:" + httpPort + "/fetch"))
            .header("Content-Type", "application/json")
            .POST(HttpRequest.BodyPublishers.ofString("{\"url\": \"" + url + "\"}"))
            .build();
        return HTTP.send(request, HttpResponse.BodyHandlers.ofString());
    }

    private static HttpResponse<String> fetchUnchecked(String url) {
        try {
            return fetch(url);
        } catch (IOException e) {
            throw new IllegalStateException(e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(e);
        }
    }

    /**
     * Requests of the site to paths starting with the prefix, to the host, in the order of arrival.
     */
    private static List<ServeEvent> served(String pathPrefix, String host) {
        return site.getAllServeEvents().stream()
            .filter(e -> e.getRequest().getUrl().startsWith(pathPrefix))
            .filter(e -> e.getRequest().getHeader("Host").startsWith(host + ":"))
            .sorted(Comparator.comparing(e -> e.getRequest().getLoggedDate()))
            .toList();
    }

    private static long started(ServeEvent event) {
        return event.getRequest().getLoggedDate().getTime();
    }

    private static String sitemap(int size) {
        final StringBuilder out = new StringBuilder(size + 64);
        for (int i = 0; out.length() < size; i++) {
            out.append("<url><loc>https://example.com/page/").append(i).append("</loc></url>\n");
        }
        out.setLength(size);
        return out.toString();
    }

    /**
     * The bucket of the sidecar, repeated until the storage takes a write: the port is open before the S3 gateway
     * answers, and its volumes start later still.
     */
    private static void createBucket() throws InterruptedException {
        final HttpRequest bucket = HttpRequest.newBuilder(s3("/" + BUCKET)).PUT(HttpRequest.BodyPublishers.noBody()).build();
        final HttpRequest probe = HttpRequest.newBuilder(s3("/" + BUCKET + "/probe"))
            .PUT(HttpRequest.BodyPublishers.ofString("probe"))
            .build();
        final long deadline = System.nanoTime() + WAIT.toNanos();
        while (send(bucket) >= 500 || send(probe) != 200) {
            if (System.nanoTime() > deadline) {
                throw new IllegalStateException("SeaweedFS takes no writes: " + storage.getLogs());
            }
            Thread.sleep(200);
        }
    }

    /**
     * Status of the answer; no answer at all - 503.
     */
    private static int send(HttpRequest request) throws InterruptedException {
        try {
            return HTTP.send(request, HttpResponse.BodyHandlers.discarding()).statusCode();
        } catch (IOException e) {
            return 503;
        }
    }

    private static URI s3(String path) {
        return URI.create("http://" + storage.getHost() + ":" + storage.getMappedPort(S3_PORT) + path);
    }
}
