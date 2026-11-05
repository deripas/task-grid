package io.github.deripas.taskgrid.worker.remote.grpc;

import io.github.deripas.taskgrid.dispatch.Dispatcher;
import io.github.deripas.taskgrid.queue.DelayPolicy;
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
import io.github.deripas.taskgrid.worker.remote.v1.SidecarMessage;
import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.Status;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * A node with the remote worker over in-process gRPC and test sidecars in place of handler-sidecar:
 * queue -> RemoteWorker -> Node.Connect -> sidecar -> Node.Complete -> queue -> result store.
 */
class RemoteNodeTest {
    private static final Duration WAIT = Duration.ofSeconds(10);

    private final ResultStore results = new MemoryResultStore(Duration.ofMinutes(1), Clock.systemUTC());
    private final List<TestSidecar> sidecars = new ArrayList<>();
    private RemoteWorker worker;
    private MemoryTaskQueue queue;
    private Server server;
    private ManagedChannel channel;

    @AfterEach
    void stop() throws InterruptedException {
        sidecars.forEach(TestSidecar::close);
        channel.shutdownNow().awaitTermination(5, TimeUnit.SECONDS);
        server.shutdownNow().awaitTermination(5, TimeUnit.SECONDS);
        worker.close();
        queue.close();
    }

    @Test
    void tasksOfAKeyRunOnTheirPreferredSidecar() throws IOException {
        start(new RemoteSettings(Duration.ofMillis(10), Duration.ofSeconds(10), Duration.ofMillis(20), Duration.ofSeconds(30)));
        final TestSidecar a = sidecar("worker-a", 4, attempt -> attempt.getPayload().toByteArray());
        final TestSidecar b = sidecar("worker-b", 4, attempt -> attempt.getPayload().toByteArray());

        final List<String> ids = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            final String id = "task-" + i;
            ids.add(id);
            submit(id, Map.of(Worker.KIND, "echo", Worker.AFFINITY_KEY, "key-" + (i % 10)));
        }

        for (String id : ids) {
            final TaskResult result = await(id);
            assertEquals(TaskStatus.DONE, result.status(), result.error());
            assertEquals(Blob.of(id.getBytes(StandardCharsets.UTF_8)), result.body());
        }
        final Map<String, Set<String>> workersByKey = new HashMap<>();
        for (TestSidecar s : List.of(a, b)) {
            s.received().forEach(attempt -> workersByKey
                .computeIfAbsent(attempt.getMetaOrThrow(Worker.AFFINITY_KEY), k -> new HashSet<>())
                .add(s.workerId()));
        }
        assertEquals(10, workersByKey.size());
        workersByKey.forEach((key, workers) -> assertEquals(1, workers.size(), key + " ran on " + workers));
        assertTrue(!a.received().isEmpty() && !b.received().isEmpty(), "keys are spread over both sidecars");
    }

    @Test
    void attemptOfACrashedSidecarIsRetriedOnAnother() throws IOException, InterruptedException {
        start(RemoteSettings.DEFAULTS);
        final CountDownLatch started = new CountDownLatch(1);
        final TestSidecar stuck = sidecar("worker-a", 1, attempt -> {
            started.countDown();
            new CountDownLatch(1).await();
            throw new IllegalStateException("unreachable");
        });
        submit("task-1", Map.of(Worker.KIND, "echo"));
        assertTrue(started.await(WAIT.toSeconds(), TimeUnit.SECONDS));

        sidecar("worker-b", 1, attempt -> "done".getBytes(StandardCharsets.UTF_8));
        stuck.crash();

        final TaskResult result = await("task-1");
        assertEquals(TaskStatus.DONE, result.status(), result.error());
        assertEquals("2", result.meta().get(TaskQueue.ATTEMPTS));
    }

    @Test
    void drainingSidecarFinishesItsAttemptAndGetsNoMore() throws IOException, InterruptedException {
        start(RemoteSettings.DEFAULTS);
        final CountDownLatch started = new CountDownLatch(1);
        final CountDownLatch release = new CountDownLatch(1);
        final TestSidecar draining = sidecar("worker-a", 2, attempt -> {
            started.countDown();
            release.await();
            return "first".getBytes(StandardCharsets.UTF_8);
        });
        submit("task-1", Map.of(Worker.KIND, "echo"));
        assertTrue(started.await(WAIT.toSeconds(), TimeUnit.SECONDS));

        draining.drain();
        final TestSidecar next = sidecar("worker-b", 1, attempt -> "second".getBytes(StandardCharsets.UTF_8));
        submit("task-2", Map.of(Worker.KIND, "echo"));
        assertEquals(TaskStatus.DONE, await("task-2").status());
        assertFalse(draining.ended().isDone(), "the session waits for its attempt in progress");

        release.countDown();
        final TaskResult first = await("task-1");
        assertEquals(TaskStatus.DONE, first.status(), first.error());
        assertEquals("1", first.meta().get(TaskQueue.ATTEMPTS));
        assertEquals(Status.Code.OK, draining.ended().orTimeout(WAIT.toSeconds(), TimeUnit.SECONDS).join().getCode());
        assertEquals(1, draining.received().size());
        assertEquals(1, next.received().size());
    }

    @Test
    void taskOfAKindWithoutSidecarsFails() throws IOException {
        start(new RemoteSettings(Duration.ofMillis(10), Duration.ofMillis(200), Duration.ofMillis(20), Duration.ofMillis(200)));
        sidecar("worker-a", 1, attempt -> new byte[0]);

        submit("task-1", Map.of(Worker.KIND, "render", "client.request-id", "r-1"));

        final TaskResult result = await("task-1");
        assertEquals(TaskStatus.FAILED, result.status());
        assertTrue(result.error().contains("no credit from sidecars of kind render"), result.error());
        assertEquals("r-1", result.meta().get("client.request-id"));
    }

    @Test
    void repeatedHelloIsAProtocolViolation() throws IOException {
        start(RemoteSettings.DEFAULTS);
        final TestSidecar s = sidecar("worker-a", 1, attempt -> new byte[0]);
        s.send(SidecarMessage.newBuilder().setHello(SidecarMessage.getDefaultInstance().getHello()).build());

        final Status status = s.ended().orTimeout(WAIT.toSeconds(), TimeUnit.SECONDS).join();
        assertEquals(Status.Code.INVALID_ARGUMENT, status.getCode());
        assertEquals("repeated hello", status.getDescription());
    }

    private void start(RemoteSettings settings) throws IOException {
        queue = new MemoryTaskQueue(new QueueSettings(3), DelayPolicy.NONE, results);
        worker = new RemoteWorker(settings, Clock.systemUTC());
        Dispatcher.connect(queue, worker);
        final String name = InProcessServerBuilder.generateName();
        server = InProcessServerBuilder.forName(name).addService(new NodeService(worker)).build().start();
        channel = InProcessChannelBuilder.forName(name).build();
    }

    private TestSidecar sidecar(
        String workerId,
        int capacity,
        TestSidecar.Handler handler
    ) {
        final TestSidecar s = new TestSidecar(channel, workerId, "echo", capacity, handler);
        sidecars.add(s);
        return s;
    }

    private void submit(String id, Map<String, String> meta) {
        queue.submit(new Task(id, Blob.of(id.getBytes(StandardCharsets.UTF_8)), meta)).toCompletableFuture().join();
    }

    private TaskResult await(String id) {
        return results.await(id, WAIT).toCompletableFuture().join().orElseThrow(() -> new AssertionError("no result for " + id));
    }
}
