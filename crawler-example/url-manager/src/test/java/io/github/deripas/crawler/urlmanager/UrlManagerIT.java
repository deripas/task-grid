package io.github.deripas.crawler.urlmanager;

import com.github.tomakehurst.wiremock.WireMockServer;
import com.github.tomakehurst.wiremock.stubbing.ServeEvent;
import io.github.deripas.crawler.fetcher.grpc.FetcherService;
import io.github.deripas.crawler.fetcher.http.ApacheFetcher;
import io.github.deripas.crawler.urlmanager.api.FetchController;
import io.github.deripas.crawler.urlmanager.app.UrlManagerApplication;
import io.github.deripas.crawler.urlmanager.amqp.RabbitFetchListener;
import io.github.deripas.crawler.urlmanager.grpc.NodeServer;
import io.github.deripas.crawler.urlmanager.v1.FetchReply;
import io.github.deripas.crawler.urlmanager.v1.FetchRequest;
import io.github.deripas.crawler.urlmanager.v1.Refusal;
import io.grpc.Grpc;
import io.grpc.InsecureServerCredentials;
import io.grpc.Server;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ConfigurableApplicationContext;
import org.testcontainers.Testcontainers;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.images.builder.ImageFromDockerfile;
import org.testcontainers.utility.MountableFile;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * End to end: client -> url-manager (this JVM: Spring application, in-memory task-grid node) -> handler-sidecar
 * (the real image) -> fetcher (this JVM) -> site (WireMock); pages above the inline limit of the sidecar go through
 * SeaweedFS. Requests come over HTTP and over RabbitMQ. Needs Docker; run by {@code make it}.
 */
class UrlManagerIT {
    private static final Duration WAIT = Duration.ofSeconds(30);
    private static final int S3_PORT = 8333;
    private static final String BUCKET = "results";
    private static final String REQUESTS = "url-manager.fetch";
    private static final String SUBMIT_REQUESTS = "url-manager.fetch.after-submit";
    private static final String CLUSTER_REQUESTS = "url-manager.fetch.cluster";
    private static final int CQL_PORT = 9042;
    private static final Duration SLOW = Duration.ofMillis(300);
    private static final Duration CRAWL_DELAY = Duration.ofMillis(500);
    private static final ImageFromDockerfile SIDECAR_IMAGE = new ImageFromDockerfile("task-grid/handler-sidecar-it", false)
        .withDockerfile(Path.of(System.getProperty("handler-sidecar.dir", "../../handler-sidecar"), "Dockerfile"));
    private static final HttpClient HTTP = HttpClient.newHttpClient();

    private static WireMockServer site;
    private static ExecutorService calls;
    private static ApacheFetcher fetcher;
    private static Server fetcherServer;
    private static Network network;
    private static GenericContainer<?> storage;
    private static GenericContainer<?> rabbit;
    private static CachingConnectionFactory client;
    private static RabbitTemplate requests;
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
        rabbit = new GenericContainer<>("rabbitmq:4.3-alpine")
            .withExposedPorts(5672)
            .waitingFor(Wait.forLogMessage(".*Server startup complete.*", 1).withStartupTimeout(WAIT));
        rabbit.start();

        urlManager = startUrlManager(REQUESTS, "after-reply");
        httpPort = urlManager.getEnvironment().getRequiredProperty("local.server.port", Integer.class);
        sidecar = startSidecar(urlManager, "fetcher-it");

        client = new CachingConnectionFactory(rabbit.getHost(), rabbit.getMappedPort(5672));
        requests = new RabbitTemplate(client);
        requests.setReplyTimeout(WAIT.toMillis());
    }

    @AfterAll
    static void stop() throws IOException, InterruptedException {
        client.destroy();
        sidecar.stop();
        urlManager.close();
        rabbit.stop();
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
     * Pages of one host are fetched one at a time, the next one not earlier than the crawl-delay after the previous
     * answer; another host does not wait for it ("localhost" and "127.0.0.1" are different hosts of the same WireMock).
     */
    @Test
    void fetchesOfAHostKeepTheCrawlDelay() throws Exception {
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
            final long expected = SLOW.toMillis() + CRAWL_DELAY.toMillis();
            assertTrue(gap >= expected - 10, "fetch " + i + " of localhost started " + gap + " ms after the previous one");
        }
        final long other = started(served("/slow/other", "127.0.0.1").getFirst());
        final long last = started(local.getLast());
        assertTrue(other < last, "the other host waited for localhost");
    }

    @Test
    void pageOverRabbit() throws Exception {
        site.stubFor(get("/rabbit").willReturn(aResponse().withHeader("Content-Type", "text/html").withBody("<html>rabbit</html>")));

        final FetchReply reply = rabbitFetch("r-1", "http://127.0.0.1:" + site.port() + "/rabbit");

        assertEquals("r-1", reply.getRequestId());
        assertEquals(200, reply.getPage().getStatus());
        assertEquals("<html>rabbit</html>", reply.getPage().getBody().toStringUtf8());
        assertTrue(reply.getPage().getHeadersList().stream().anyMatch(h -> h.getName().equalsIgnoreCase("Content-Type")));
    }

    /**
     * Read from SeaweedFS and sent in the reply as is.
     */
    @Test
    void largePageOverRabbit() throws Exception {
        final String sitemap = sitemap(8 << 20);
        site.stubFor(get("/rabbit/sitemap.xml").willReturn(aResponse().withBody(sitemap)));

        final FetchReply reply = rabbitFetch("r-2", "http://127.0.0.1:" + site.port() + "/rabbit/sitemap.xml");

        assertEquals(sitemap, reply.getPage().getBody().toStringUtf8());
    }

    /**
     * The queue of tasks is in memory: the request stays unacknowledged in RabbitMQ until its reply is confirmed.
     */
    @Test
    void requestIsAcknowledgedOnlyAfterTheReply() throws Exception {
        site.stubFor(get("/rabbit/slow").willReturn(aResponse().withFixedDelay(1_500).withBody("slow")));

        final CompletableFuture<FetchReply> reply = CompletableFuture.supplyAsync(() -> {
            try {
                return rabbitFetch("r-4", "http://127.0.0.1:" + site.port() + "/rabbit/slow");
            } catch (Exception e) {
                throw new IllegalStateException(e);
            }
        }, calls);
        final long deadline = System.nanoTime() + WAIT.toNanos();
        while (unacknowledged() == 0 && !reply.isDone() && System.nanoTime() < deadline) {
            Thread.sleep(50);
        }
        assertEquals(1, unacknowledged(), "request in work is not held by the broker");

        assertEquals("slow", reply.get(WAIT.toSeconds(), TimeUnit.SECONDS).getPage().getBody().toStringUtf8());
        while (unacknowledged() != 0 && System.nanoTime() < deadline) {
            Thread.sleep(50);
        }
        assertEquals(0, unacknowledged());
    }

    @Test
    void badUrlOverRabbitIsRefused() throws Exception {
        final FetchReply reply = rabbitFetch("r-3", "ftp://example.com/");

        assertEquals(Refusal.Code.CODE_INVALID, reply.getRefusal().getCode());
    }

    /**
     * The target way on a url-manager of its own (queue {@value #SUBMIT_REQUESTS}, its own sidecar): the request is
     * acknowledged while its page is still fetched, the reply comes from the result callback.
     */
    @Test
    void ackAfterSubmitRepliesFromTheResultCallback() throws Exception {
        site.stubFor(get("/rabbit/submit").willReturn(aResponse().withFixedDelay(1_500).withBody("submitted")));
        try (ConfigurableApplicationContext target = startUrlManager(SUBMIT_REQUESTS, "after-submit")) {
            final GenericContainer<?> targetSidecar = startSidecar(target, "fetcher-it-submit");
            try {
                final CompletableFuture<FetchReply> reply = CompletableFuture.supplyAsync(() -> {
                    try {
                        return rabbitFetch(SUBMIT_REQUESTS, "r-5", "http://127.0.0.1:" + site.port() + "/rabbit/submit");
                    } catch (Exception e) {
                        throw new IllegalStateException(e);
                    }
                }, calls);
                final long deadline = System.nanoTime() + WAIT.toNanos();
                while (site.getAllServeEvents().stream().noneMatch(e -> e.getRequest().getUrl().equals("/rabbit/submit"))
                    && System.nanoTime() < deadline) {
                    Thread.sleep(50);
                }
                assertFalse(reply.isDone(), "the page is still fetched");
                assertEquals(0, unacknowledged(SUBMIT_REQUESTS), "the request is acknowledged once queued");

                final FetchReply page = reply.get(WAIT.toSeconds(), TimeUnit.SECONDS);
                assertEquals("r-5", page.getRequestId());
                assertEquals("submitted", page.getPage().getBody().toStringUtf8());
            } finally {
                targetSidecar.stop();
            }
        }
    }

    /**
     * Cluster mode on ScyllaDB: requests over RabbitMQ (after-submit) survive a restart of url-manager. A is fetched
     * while the node stops, B waits behind it for the crawl-delay of the host; a new node (the same gRPC port, the same
     * journal) recovers the partition, takes the result of A the sidecar sends again, replies to A, then fetches B.
     */
    @Test
    void clusterModeKeepsRequestsOverARestart() throws Exception {
        site.stubFor(get("/cluster/a").willReturn(aResponse().withFixedDelay(3_000).withBody("page a")));
        site.stubFor(get("/cluster/b").willReturn(aResponse().withBody("page b")));
        try (GenericContainer<?> scylla = new GenericContainer<>("scylladb/scylla:2026.3")
            .withCommand("--smp", "1", "--memory", "512M", "--overprovisioned", "1", "--developer-mode", "1")
            .withExposedPorts(CQL_PORT)
            .waitingFor(Wait.forLogMessage(".*(Starting listening for CQL clients|init - serving).*", 1)
                .withStartupTimeout(Duration.ofMinutes(3)))) {
            scylla.start();
            applySchema(scylla);
            final int grpcPort = freePort();
            final String[] cluster = {
                "--url-manager.queue=cluster",
                "--url-manager.grpc-port=" + grpcPort,
                "--url-manager.cluster.port=0",
                "--url-manager.cluster.contact-points=" + scylla.getHost() + ":" + scylla.getMappedPort(CQL_PORT)};
            ConfigurableApplicationContext node = startUrlManager(CLUSTER_REQUESTS, "after-submit", cluster);
            final GenericContainer<?> clusterSidecar = startSidecar(node, "fetcher-it-cluster");
            try {
                final String host = "http://127.0.0.1:" + site.port();
                final CompletableFuture<FetchReply> a = rabbitFetchAsync("c-a", host + "/cluster/a");
                final CompletableFuture<FetchReply> b = rabbitFetchAsync("c-b", host + "/cluster/b");
                final long deadline = System.nanoTime() + WAIT.toNanos();
                while (site.getAllServeEvents().stream().noneMatch(e -> e.getRequest().getUrl().equals("/cluster/a"))
                    || unacknowledged(CLUSTER_REQUESTS) > 0) {
                    assertTrue(System.nanoTime() < deadline, "A not in work within " + WAIT);
                    Thread.sleep(50);
                }

                node.close();
                node = startUrlManager(CLUSTER_REQUESTS, "after-submit", cluster);

                assertEquals("page a", a.get(WAIT.toSeconds(), TimeUnit.SECONDS).getPage().getBody().toStringUtf8());
                assertEquals("page b", b.get(WAIT.toSeconds(), TimeUnit.SECONDS).getPage().getBody().toStringUtf8());
                assertEquals(1, served("/cluster/a", "127.0.0.1").size(), "A is not fetched again");
            } finally {
                clusterSidecar.stop();
                node.close();
            }
        }
    }

    /**
     * url-manager of this JVM on the shared site, storage and broker; nodePort and httpPort - any free ones.
     */
    private static ConfigurableApplicationContext startUrlManager(
        String queue,
        String ack,
        String... more
    ) {
        final List<String> args = new ArrayList<>(List.of(
            "--server.port=0",
            "--url-manager.grpc-port=0",
            "--url-manager.crawl-delay=" + CRAWL_DELAY.toMillis() + "ms",
            "--url-manager.blob.endpoint=" + s3(""),
            "--url-manager.blob.access-key=any",
            "--url-manager.blob.secret-key=any",
            "--url-manager.rabbit.enabled=true",
            "--url-manager.rabbit.queue=" + queue,
            "--url-manager.rabbit.ack=" + ack,
            "--spring.rabbitmq.host=" + rabbit.getHost(),
            "--spring.rabbitmq.port=" + rabbit.getMappedPort(5672)));
        for (String arg : more) {
            // a repeated argument would be joined into a list: the later one replaces the default
            final String name = arg.substring(0, arg.indexOf('=') + 1);
            args.removeIf(defaultArg -> defaultArg.startsWith(name));
            args.add(arg);
        }
        return new SpringApplicationBuilder(UrlManagerApplication.class).run(args.toArray(String[]::new));
    }

    /**
     * The real sidecar in front of the fetcher of this JVM, connected to the node of the url-manager.
     */
    private static GenericContainer<?> startSidecar(ConfigurableApplicationContext node, String workerId) {
        final int nodePort = node.getBean(NodeServer.class).port();
        Testcontainers.exposeHostPorts(nodePort, fetcherServer.getPort());
        final GenericContainer<?> container = new GenericContainer<>(SIDECAR_IMAGE)
            .withNetwork(network)
            .withEnv(Map.of(
                "NODE_ADDRESS", "host.testcontainers.internal:" + nodePort,
                "HANDLER_TARGET", "host.testcontainers.internal:" + fetcherServer.getPort(),
                "HANDLER_METHOD", "/crawler.fetcher.v1.Fetcher/Fetch",
                "WORKER_ID", workerId,
                "KIND", "fetch",
                "CAPACITY", "4",
                "INLINE_MAX", String.valueOf(64 << 10),
                "BLOB_S3_ENDPOINT", "seaweedfs:" + S3_PORT,
                "BLOB_PREFIX", "it/"))
            .waitingFor(Wait.forLogMessage(".*session open.*", 1).withStartupTimeout(WAIT));
        container.start();
        return container;
    }

    private static CompletableFuture<FetchReply> rabbitFetchAsync(String id, String url) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                return rabbitFetch(CLUSTER_REQUESTS, id, url);
            } catch (Exception e) {
                throw new IllegalStateException(e);
            }
        }, calls);
    }

    /**
     * cql/schema.cql of url-manager, as the init container of the stand applies it; cqlsh waits for the node to serve.
     */
    private static void applySchema(GenericContainer<?> scylla) throws IOException, InterruptedException {
        scylla.copyFileToContainer(MountableFile.forHostPath(Path.of("cql/schema.cql")), "/schema.cql");
        final long deadline = System.nanoTime() + Duration.ofMinutes(2).toNanos();
        while (true) {
            final Container.ExecResult result = scylla.execInContainer("cqlsh", "-f", "/schema.cql");
            if (result.getExitCode() == 0) {
                return;
            }
            assertTrue(System.nanoTime() < deadline, "schema not applied: " + result.getStderr());
            Thread.sleep(2_000);
        }
    }

    private static int freePort() throws IOException {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        }
    }

    private static FetchReply rabbitFetch(String id, String url) throws Exception {
        return rabbitFetch(REQUESTS, id, url);
    }

    /**
     * Request and reply as a client sends them: the reply comes to the direct reply-to of the template.
     */
    private static FetchReply rabbitFetch(
        String queue,
        String id,
        String url
    ) throws Exception {
        final MessageProperties properties = new MessageProperties();
        properties.setContentType(RabbitFetchListener.CONTENT_TYPE);
        final byte[] body = FetchRequest.newBuilder().setRequestId(id).setUrl(url).build().toByteArray();
        final Message reply = requests.sendAndReceive("", queue, new Message(body, properties));
        assertTrue(reply != null, "no reply within " + WAIT);
        return FetchReply.parseFrom(reply.getBody());
    }

    /**
     * Unacknowledged messages of the request queue, as the broker counts them.
     */
    private static int unacknowledged() throws IOException, InterruptedException {
        return unacknowledged(REQUESTS);
    }

    private static int unacknowledged(String queue) throws IOException, InterruptedException {
        final String out = rabbit.execInContainer("rabbitmqctl", "list_queues", "-q", "name", "messages_unacknowledged").getStdout();
        return out.lines()
            .map(line -> line.trim().split("\\s+"))
            .filter(columns -> columns.length == 2 && columns[0].equals(queue))
            .mapToInt(columns -> Integer.parseInt(columns[1]))
            .findFirst()
            .orElse(0);
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
