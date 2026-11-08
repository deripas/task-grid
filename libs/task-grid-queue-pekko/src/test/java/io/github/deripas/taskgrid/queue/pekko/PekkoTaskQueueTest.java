package io.github.deripas.taskgrid.queue.pekko;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import io.github.deripas.taskgrid.dispatch.Dispatcher;
import io.github.deripas.taskgrid.queue.DelayPolicy;
import io.github.deripas.taskgrid.queue.QueueSettings;
import io.github.deripas.taskgrid.queue.TaskQueue;
import io.github.deripas.taskgrid.queue.partition.PartitionSnapshot;
import io.github.deripas.taskgrid.task.Attempt;
import io.github.deripas.taskgrid.task.AttemptResult;
import io.github.deripas.taskgrid.task.Blob;
import io.github.deripas.taskgrid.task.Request;
import io.github.deripas.taskgrid.task.RequestId;
import io.github.deripas.taskgrid.task.Task;
import io.github.deripas.taskgrid.task.TaskResult;
import io.github.deripas.taskgrid.task.TaskStatus;
import io.github.deripas.taskgrid.worker.Worker;
import io.github.deripas.taskgrid.worker.local.LocalWorker;
import org.apache.pekko.actor.testkit.typed.javadsl.ActorTestKit;
import org.apache.pekko.actor.testkit.typed.javadsl.TestProbe;
import org.apache.pekko.cluster.sharding.ShardRegion;
import org.apache.pekko.cluster.sharding.typed.GetShardRegionState;
import org.apache.pekko.cluster.sharding.typed.javadsl.ClusterSharding;
import org.apache.pekko.cluster.typed.Cluster;
import org.apache.pekko.cluster.typed.Join;
import org.apache.pekko.japi.Pair;
import org.apache.pekko.persistence.testkit.PersistenceTestKitPlugin;
import org.apache.pekko.persistence.testkit.PersistenceTestKitSnapshotPlugin;
import org.apache.pekko.persistence.testkit.SnapshotMeta;
import org.apache.pekko.persistence.testkit.javadsl.SnapshotTestKit;
import org.apache.pekko.persistence.typed.PersistenceId;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The queue on a one-node cluster with the journal and snapshots in memory. Every message, event and snapshot goes
 * through its serializer, as between nodes and into a real journal.
 */
class PekkoTaskQueueTest {
    private static final Duration PAUSE = Duration.ofMillis(300);
    private static final Duration WAIT = Duration.ofSeconds(10);
    private static final Duration IDLE = Duration.ofSeconds(1);

    private final Map<String, CompletableFuture<TaskResult>> results = new ConcurrentHashMap<>();
    private final BlockingQueue<Attempt> ready = new LinkedBlockingQueue<>();
    private ActorTestKit testKit;
    private PekkoTaskQueue queue;

    @AfterEach
    void stop() {
        testKit.shutdownTestKit();
    }

    @Test
    void tasksOfAKeyRunOneAtATimeWithThePauseBetween() throws InterruptedException {
        start(new QueueSettings(3, Duration.ofMinutes(1)), DelayPolicy.fixed(PAUSE));
        submit(task("a1", "a.com"));
        submit(task("a2", "a.com"));
        submit(task("b1", "b.com"));

        assertEquals(Set.of("a1", "b1"), Set.of(take().task().id(), take().task().id()));
        assertTrue(ready.poll(200, TimeUnit.MILLISECONDS) == null, "a2 waits for a1");

        queue.complete(AttemptResult.done(new Attempt(task("a1", "a.com"), 1), Blob.of(new byte[]{1})));
        final long finished = System.nanoTime();
        final Attempt a2 = take();
        final long waited = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - finished);

        assertEquals("a2", a2.task().id());
        assertTrue(waited >= PAUSE.toMillis() - 20, "a2 after " + waited + " ms");
        assertEquals(TaskStatus.DONE, result("a1").status());
    }

    @Test
    void anotherPayloadUnderAQueuedIdIsRejected() {
        start(new QueueSettings(3, Duration.ofMinutes(1)), DelayPolicy.NONE);
        submit(task("t", "a.com"));
        submit(task("t", "a.com"));

        final CompletionException error = assertThrows(CompletionException.class, () -> queue.submit(
            new Task("t", Blob.of(new byte[]{9}), Map.of(TaskQueue.PARTITION_KEY, "a.com")), request("x")).toCompletableFuture().join());
        assertInstanceOf(IllegalArgumentException.class, error.getCause());
    }

    /**
     * Requests of the same task in the partition: one attempt, the result to each of them.
     */
    @Test
    void requestsOfTheSameTaskAreDoneOnce() throws InterruptedException {
        start(new QueueSettings(3, Duration.ofMinutes(1)), DelayPolicy.NONE);
        final Task task = Task.of(Blob.of(new byte[]{7}), Map.of(TaskQueue.PARTITION_KEY, "a.com"));
        queue.submit(task, request("r1")).toCompletableFuture().join();
        queue.submit(task, request("r2")).toCompletableFuture().join();

        queue.complete(AttemptResult.done(take(), Blob.of(new byte[0]))).toCompletableFuture().join();
        assertTrue(ready.poll(200, TimeUnit.MILLISECONDS) == null, "one attempt");
        assertEquals(result("r1"), result("r2"));
    }

    /**
     * A receiver fails: the final result is not taken - the worker repeats it until the delivery succeeds.
     */
    @Test
    void completeFailsUntilTheResultIsDelivered() throws InterruptedException {
        start(new QueueSettings(3, Duration.ofMinutes(1)), DelayPolicy.NONE);
        final AtomicInteger calls = new AtomicInteger();
        queue.onResult((result, requests) -> calls.incrementAndGet() < 3
            ? CompletableFuture.failedFuture(new IllegalStateException("broker is down"))
            : CompletableFuture.completedFuture(null));
        submit(task("t", null));
        final AttemptResult done = AttemptResult.done(take(), Blob.of(new byte[0]));

        assertThrows(CompletionException.class, () -> queue.complete(done).toCompletableFuture().join());
        assertThrows(CompletionException.class, () -> queue.complete(done).toCompletableFuture().join());
        queue.complete(done).toCompletableFuture().join();

        assertEquals(3, calls.get());
        assertEquals(TaskStatus.DONE, result("t").status());
        queue.complete(done).toCompletableFuture().join();
        assertEquals(3, calls.get(), "a repeated result of a finished task is ignored");
    }

    /**
     * Attempts exhausted by the deadline while the receiver is down: the failure is decided again until delivered.
     */
    @Test
    void resultOfADeadlineIsDecidedAgainUntilDelivered() throws InterruptedException {
        start(new QueueSettings(1, Duration.ofMillis(300)), DelayPolicy.NONE);
        final AtomicInteger calls = new AtomicInteger();
        final CompletableFuture<TaskResult> delivered = new CompletableFuture<>();
        queue.onResult((result, requests) -> {
            if (calls.incrementAndGet() < 3) {
                return CompletableFuture.failedFuture(new IllegalStateException("broker is down"));
            }
            delivered.complete(result);
            return CompletableFuture.completedFuture(null);
        });
        submit(task("t", "a.com"));

        assertEquals(TaskStatus.FAILED, delivered.orTimeout(WAIT.toMillis(), TimeUnit.MILLISECONDS).join().status());
        Thread.sleep(300);
        assertEquals(3, calls.get(), "delivered once it succeeded");
    }

    /**
     * The worker never answers (its node is gone): after the attempt timeout the attempt is handed out again.
     */
    @Test
    void attemptWithoutAResultIsHandedOutAgainAfterTheTimeout() throws InterruptedException {
        start(new QueueSettings(3, Duration.ofMillis(300)), DelayPolicy.NONE);
        submit(task("t", "a.com"));

        assertEquals(1, take().number());
        assertEquals(2, take().number());
    }

    /**
     * The entity stops (its node is gone, a rebalance) and recovers from the journal and snapshots: the attempt in
     * progress is not handed out again and takes its result; the pause, the waiting tasks and their requests survive.
     */
    @Test
    void stateSurvivesARestartOfThePartition() throws InterruptedException {
        start(new QueueSettings(3, Duration.ofMinutes(1)), DelayPolicy.fixed(PAUSE));
        for (int i = 1; i <= 5; i++) {
            submit(task("a" + i, "a.com"));
        }
        final Attempt a1 = take();
        restart("a.com");
        assertTrue(ready.poll(500, TimeUnit.MILLISECONDS) == null, "the attempt in progress is not handed out again");

        queue.complete(AttemptResult.done(a1, Blob.of(new byte[]{1})));
        assertEquals(TaskStatus.DONE, result("a1").status());
        final Attempt a2 = take();
        assertEquals("a2", a2.task().id());

        restart("a.com");
        queue.complete(AttemptResult.done(a2, Blob.of(new byte[]{2})));
        assertEquals(TaskStatus.DONE, result("a2").status());
        assertEquals("a3", take().task().id());
    }

    /**
     * An empty partition stays for the idle timeout - a task coming within it finds it in memory - then stops.
     */
    @Test
    void emptyPartitionStaysForTheIdleTimeoutThenStops() throws InterruptedException {
        start(new QueueSettings(3, Duration.ofMinutes(1)), DelayPolicy.NONE);
        submit(task("a1", "a.com"));
        queue.complete(AttemptResult.done(take(), Blob.of(new byte[0])));
        result("a1");
        final String entity = PartitionEntity.entityId("-", Map.of(TaskQueue.PARTITION_KEY, "a.com"));

        Thread.sleep(IDLE.toMillis() / 2);
        assertTrue(entities().contains(entity), "still there within the idle timeout");

        final long deadline = System.nanoTime() + WAIT.toNanos();
        while (entities().contains(entity)) {
            assertTrue(System.nanoTime() < deadline, "not stopped within " + WAIT);
            Thread.sleep(100);
        }
        // the empty state is the last snapshot: the next recovery reads only it
        final List<Pair<SnapshotMeta, Object>> snapshots = SnapshotTestKit.create(testKit.system())
            .persistedInStorage(PersistenceId.of(PartitionEntity.TYPE_KEY.name(), entity).id());
        assertEquals(new PartitionSnapshot(List.of(), null, 0, false, 0, snapshotOf(snapshots).resumeAt()),
            snapshotOf(snapshots));

        submit(task("a2", "a.com"));
        queue.complete(AttemptResult.done(take(), Blob.of(new byte[0])));
        assertEquals(TaskStatus.DONE, result("a2").status());
    }

    private static PartitionSnapshot snapshotOf(List<Pair<SnapshotMeta, Object>> snapshots) {
        assertTrue(!snapshots.isEmpty(), "no snapshot");
        return (PartitionSnapshot) snapshots.getLast().second();
    }

    /**
     * The whole flow with a worker: tasks of a key in order, keys in parallel, all results back.
     */
    @Test
    void tasksGoThroughTheDispatcherToTheWorkerAndBack() {
        start(new QueueSettings(3, Duration.ofMinutes(1)), DelayPolicy.NONE);
        final Map<String, ConcurrentLinkedQueue<String>> order = new ConcurrentHashMap<>();
        final ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor();
        try {
            Dispatcher.connect(queue, new LocalWorker(Map.of("echo", attempt -> {
                order.computeIfAbsent(attempt.task().meta().get(TaskQueue.PARTITION_KEY), k -> new ConcurrentLinkedQueue<>())
                    .add(attempt.task().id());
                return attempt.task().payload();
            }), executor));
            final Map<String, List<String>> submitted = new HashMap<>();
            final List<String> ids = new ArrayList<>();
            for (int i = 0; i < 60; i++) {
                final String key = "key-" + (i % 6);
                final String id = "task-" + i;
                final Map<String, String> meta = Map.of(Worker.KIND, "echo", TaskQueue.PARTITION_KEY, key);
                submit(new Task(id, Blob.of(id.getBytes(StandardCharsets.UTF_8)), meta));
                submitted.computeIfAbsent(key, k -> new ArrayList<>()).add(id);
                ids.add(id);
            }
            for (String id : ids) {
                assertEquals(TaskStatus.DONE, result(id).status());
            }
            submitted.forEach((key, list) -> assertEquals(list, List.copyOf(order.get(key))));
        } finally {
            executor.shutdownNow();
        }
    }

    private void start(QueueSettings settings, DelayPolicy delay) {
        testKit = ActorTestKit.create("queue", config());
        queue = PekkoTaskQueue.start(testKit.system(), settings, delay);
        queue.onReady(ready::add);
        queue.onResult((result, requests) -> {
            requests.forEach(request -> future(request.id().id()).complete(result));
            return CompletableFuture.completedFuture(null);
        });
        final Cluster cluster = Cluster.get(testKit.system());
        cluster.manager().tell(Join.create(cluster.selfMember().address()));
    }

    /**
     * Stops the entity of the key; remembered, it is started again and recovers.
     */
    private void restart(String key) throws InterruptedException {
        ClusterSharding.get(testKit.system())
            .entityRefFor(PartitionEntity.TYPE_KEY, PartitionEntity.entityId("-", Map.of(TaskQueue.PARTITION_KEY, key)))
            .tell(new PartitionEntity.Stop());
        Thread.sleep(300);
    }

    private Set<String> entities() {
        final TestProbe<ShardRegion.CurrentShardRegionState> probe = testKit.createTestProbe();
        ClusterSharding.get(testKit.system()).shardState().tell(new GetShardRegionState(PartitionEntity.TYPE_KEY, probe.ref()));
        return probe.receiveMessage().getShards().stream()
            .flatMap(shard -> shard.getEntityIds().stream())
            .collect(Collectors.toSet());
    }

    private void submit(Task task) {
        queue.submit(task, request(task.id())).toCompletableFuture().join();
    }

    private Attempt take() throws InterruptedException {
        final Attempt attempt = ready.poll(WAIT.toSeconds(), TimeUnit.SECONDS);
        assertNotNull(attempt, "no attempt within " + WAIT);
        return attempt;
    }

    /**
     * The result for the request with the id.
     */
    private TaskResult result(String id) {
        return future(id).orTimeout(WAIT.toMillis(), TimeUnit.MILLISECONDS).join();
    }

    private CompletableFuture<TaskResult> future(String requestId) {
        return results.computeIfAbsent(requestId, id -> new CompletableFuture<>());
    }

    private static Request request(String id) {
        return Request.of(new RequestId("test", "0", id));
    }

    private static Task task(String id, String key) {
        final Map<String, String> meta = key == null
            ? Map.of()
            : Map.of(TaskQueue.PARTITION_KEY, key);
        return new Task(id, Blob.of(id.getBytes(StandardCharsets.UTF_8)), meta);
    }

    private static Config config() {
        return ConfigFactory.parseString("""
                pekko.actor.provider = cluster
                pekko.remote.artery.canonical.hostname = 127.0.0.1
                pekko.remote.artery.canonical.port = 0
                pekko.loggers = ["org.apache.pekko.event.slf4j.Slf4jLogger"]
                pekko.logging-filter = "org.apache.pekko.event.slf4j.Slf4jLoggingFilter"
                # every message is serialized, also the local ones: what goes between nodes must
                pekko.actor.serialize-messages = on
                pekko.cluster.jmx.multi-mbeans-in-same-jvm = on
                pekko.log-dead-letters = off
                pekko.cluster.sharding.entity-restart-backoff = 100ms
                task-grid.queue.pekko.redeliver-after = 100ms
                task-grid.queue.pekko.snapshot-every = 3
                task-grid.queue.pekko.idle-timeout = 1s
                """)
            .withFallback(PersistenceTestKitPlugin.getInstance().config())
            .withFallback(PersistenceTestKitSnapshotPlugin.config())
            .withFallback(ConfigFactory.load());
    }
}
