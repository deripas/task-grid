package io.github.deripas.taskgrid.worker.remote.grpc;

import com.github.luben.zstd.Zstd;
import io.github.deripas.taskgrid.queue.QueueSettings;
import io.github.deripas.taskgrid.queue.TaskQueue;
import io.github.deripas.taskgrid.queue.memory.MemoryTaskQueue;
import io.github.deripas.taskgrid.result.ResultStore;
import io.github.deripas.taskgrid.result.memory.MemoryResultStore;
import io.github.deripas.taskgrid.task.Blob;
import io.github.deripas.taskgrid.task.Task;
import io.github.deripas.taskgrid.task.TaskResult;
import io.github.deripas.taskgrid.task.TaskStatus;
import io.github.deripas.taskgrid.worker.Worker;
import io.github.deripas.taskgrid.worker.remote.RemoteSettings;
import io.github.deripas.taskgrid.worker.remote.RemoteWorker;
import io.grpc.Grpc;
import io.grpc.InsecureServerCredentials;
import io.grpc.Server;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
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
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Compatibility of the two sides of remote.proto: a node of this JVM (queue, RemoteWorker, NodeService over TCP)
 * and the real handler-sidecar image, built from handler-sidecar/Dockerfile, in front of a test handler of this JVM;
 * answers above the inline limit go to SeaweedFS over S3. Needs Docker; run by {@code make it}.
 */
class SidecarCompatibilityIT {
    private static final Duration WAIT = Duration.ofSeconds(30);
    private static final int CAPACITY = 4;
    private static final int INLINE_MAX = 64 << 10;
    private static final int S3_PORT = 8333;
    private static final String BUCKET = "results";
    private static final ImageFromDockerfile IMAGE = new ImageFromDockerfile("task-grid/handler-sidecar-it", false)
        .withDockerfile(Path.of(System.getProperty("handler-sidecar.dir", "../../handler-sidecar"), "Dockerfile"));

    private static final HttpClient HTTP = HttpClient.newHttpClient();
    private static Network network;
    private static GenericContainer<?> storage;

    private final ResultStore results = new MemoryResultStore(Duration.ofMinutes(1), Clock.systemUTC());
    private RemoteWorker worker;
    private TaskQueue queue;
    private Server node;
    private TestHandler handler;
    private GenericContainer<?> sidecar;

    @BeforeAll
    static void startStorage() throws InterruptedException {
        network = Network.newNetwork();
        storage = new GenericContainer<>("chrislusf/seaweedfs:4.48")
            .withCommand("server", "-s3", "-dir=/data", "-master.volumeSizeLimitMB=64", "-volume.max=0")
            .withNetwork(network)
            .withNetworkAliases("seaweedfs")
            .withExposedPorts(S3_PORT)
            .waitingFor(Wait.forListeningPorts(S3_PORT).withStartupTimeout(WAIT));
        storage.start();
        createBucket();
    }

    @AfterAll
    static void stopStorage() {
        storage.stop();
        network.close();
    }

    @BeforeEach
    void start() throws IOException {

        queue = new MemoryTaskQueue(new QueueSettings(3), results, callback -> {
            worker = new RemoteWorker(RemoteSettings.DEFAULTS, callback, Clock.systemUTC());
            return worker;
        });
        node = Grpc.newServerBuilderForPort(0, InsecureServerCredentials.create())
            .addService(new NodeService(worker))
            .build()
            .start();
        handler = new TestHandler();
        Testcontainers.exposeHostPorts(node.getPort(), handler.port());
        sidecar = new GenericContainer<>(IMAGE)
            .withNetwork(network)
            .withEnv(Map.of(
                "NODE_ADDRESS", "host.testcontainers.internal:" + node.getPort(),
                "HANDLER_TARGET", "host.testcontainers.internal:" + handler.port(),
                "HANDLER_METHOD", "/" + TestHandler.METHOD,
                "WORKER_ID", "sidecar-it",
                "KIND", "echo",
                "CAPACITY", String.valueOf(CAPACITY),
                "DRAIN_TIMEOUT", "20s",
                "INLINE_MAX", String.valueOf(INLINE_MAX),
                "BLOB_S3_ENDPOINT", "seaweedfs:" + S3_PORT,
                "BLOB_PREFIX", "it/"))
            .waitingFor(Wait.forLogMessage(".*session open.*", 1).withStartupTimeout(WAIT));
        sidecar.start();
    }

    @AfterEach
    void stop() throws InterruptedException {
        sidecar.stop();
        handler.close();
        node.shutdownNow().awaitTermination(5, TimeUnit.SECONDS);
        worker.close();
    }

    @Test
    void tasksGoThroughTheSidecarToTheHandlerAndBack() {
        final List<String> ids = new ArrayList<>();
        for (int i = 0; i < 40; i++) {
            final String id = "task-" + i;
            ids.add(id);
            submit(id, "sleep:20", Map.of(Worker.KIND, "echo", Worker.AFFINITY_KEY, "key-" + (i % 8), "client.request-id", "r-" + i));
        }

        for (int i = 0; i < ids.size(); i++) {
            final TaskResult result = await(ids.get(i));
            assertEquals(TaskStatus.DONE, result.status(), result.error());
            final String answer = new String(((Blob.Inline) result.body()).bytes(), StandardCharsets.UTF_8);
            assertEquals(List.of(
                "sleep:20",
                "x-attempt=1",
                "x-meta-affinity-key=key-" + (i % 8),
                "x-meta-client.request-id=r-" + i,
                "x-meta-kind=echo",
                "x-task-id=task-" + i), answer.lines().toList());
            assertEquals("r-" + i, result.meta().get("client.request-id"));
        }
        assertEquals(40, handler.calls());
        assertTrue(handler.peak() <= CAPACITY, "peak " + handler.peak() + " above capacity " + CAPACITY);
    }

    /**
     * 8 MiB - above the inbound limit of the node (4 MiB) too: only the reference goes through Complete.
     */
    @Test
    void largeAnswerGoesToBlobStorage() throws IOException, InterruptedException {
        final String command = "big:" + (8 << 20);
        submit("task-big", command, Map.of(Worker.KIND, "echo"));

        final TaskResult result = await("task-big");
        assertEquals(TaskStatus.DONE, result.status(), result.error());
        final Blob.Ref ref = (Blob.Ref) result.body();
        assertEquals("zstd", ref.encoding());
        assertTrue(ref.location().startsWith("s3://" + BUCKET + "/it/task-big/"), ref.location());

        final byte[] stored = read(ref.location());
        assertTrue(stored.length < ref.size() / 4, "stored " + stored.length + " of " + ref.size());
        final byte[] answer = Zstd.decompress(stored, Math.toIntExact(ref.size()));
        assertEquals(ref.size(), answer.length);
        assertEquals(ref.hash(), Blob.of(answer).hash());
        final String text = new String(answer, StandardCharsets.UTF_8);
        assertTrue(text.startsWith(command + "\n" + TestHandler.text(8 << 20)), "answer of " + text.length());
        assertTrue(text.endsWith("x-task-id=task-big"), text.substring(text.length() - 100));
    }

    @Test
    void handlerErrorFailsTheTask() {
        submit("task-1", "fail", Map.of(Worker.KIND, "echo"));

        final TaskResult result = await("task-1");
        assertEquals(TaskStatus.FAILED, result.status());
        assertTrue(result.error().contains("InvalidArgument"), result.error());
        assertEquals("1", result.meta().get(TaskQueue.ATTEMPTS));
    }

    @Test
    void sigtermDrainsTheAttemptInProgress() throws InterruptedException {
        submit("task-1", "sleep:2000", Map.of(Worker.KIND, "echo"));
        final long deadline = System.nanoTime() + WAIT.toNanos();
        while (handler.calls() == 0 && System.nanoTime() < deadline) {
            Thread.sleep(10);
        }
        assertEquals(1, handler.calls());

        // docker stop: SIGTERM, then waits for the exit; the container is kept to read its exit code
        final String id = sidecar.getContainerId();
        final CompletableFuture<Void> stopped = CompletableFuture.runAsync(() ->
            sidecar.getDockerClient().stopContainerCmd(id).withTimeout((int) WAIT.toSeconds()).exec());

        final TaskResult result = await("task-1");
        assertEquals(TaskStatus.DONE, result.status(), result.error());
        assertEquals("1", result.meta().get(TaskQueue.ATTEMPTS));
        stopped.orTimeout(WAIT.toSeconds(), TimeUnit.SECONDS).join();
        final Long exitCode = sidecar.getDockerClient().inspectContainerCmd(id).exec().getState().getExitCodeLong();
        assertEquals(0L, exitCode, sidecar.getLogs());
        assertTrue(sidecar.getLogs().contains("drained"), sidecar.getLogs());
    }

    private void submit(
        String id,
        String command,
        Map<String, String> meta
    ) {
        queue.submit(new Task(id, Blob.of(command.getBytes(StandardCharsets.UTF_8)), meta)).toCompletableFuture().join();
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
     * Object of a location s3://bucket/key, read anonymously.
     */
    private static byte[] read(String location) throws IOException, InterruptedException {
        final HttpRequest get = HttpRequest.newBuilder(s3("/" + location.substring("s3://".length()))).GET().build();
        final HttpResponse<byte[]> response = HTTP.send(get, HttpResponse.BodyHandlers.ofByteArray());
        assertEquals(200, response.statusCode(), location);
        return response.body();
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

    private TaskResult await(String id) {
        return results.await(id, WAIT).toCompletableFuture().join().orElseThrow(() -> new AssertionError("no result for " + id));
    }
}
