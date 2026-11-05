package io.github.deripas.taskgrid.queue.memory;

import io.github.deripas.taskgrid.queue.DelayPolicy;
import io.github.deripas.taskgrid.queue.QueueSettings;
import io.github.deripas.taskgrid.queue.TaskQueue;
import io.github.deripas.taskgrid.result.memory.MemoryResultStore;
import io.github.deripas.taskgrid.task.Attempt;
import io.github.deripas.taskgrid.task.AttemptResult;
import io.github.deripas.taskgrid.task.Blob;
import io.github.deripas.taskgrid.task.Task;
import io.github.deripas.taskgrid.task.TaskResult;
import io.github.deripas.taskgrid.task.TaskStatus;
import io.github.deripas.taskgrid.worker.Worker;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class MemoryTaskQueueTest {
    private static final Duration PAUSE = Duration.ofMillis(200);

    private final List<Attempt> assigned = new CopyOnWriteArrayList<>();
    private final MemoryResultStore results = new MemoryResultStore(Duration.ofMinutes(1), Clock.systemUTC());
    private final MemoryTaskQueue queue = new MemoryTaskQueue(new QueueSettings(3, Duration.ofMinutes(1)), DelayPolicy.NONE, results);

    @BeforeEach
    void subscribe() {
        queue.onReady(assigned::add);
    }

    @AfterEach
    void close() {
        queue.close();
    }

    @Test
    void resubmissionWithTheSamePayloadIsNotQueuedAgain() {
        submit(task("t", "payload"));
        submit(task("t", "payload"));

        assertEquals(1, assigned.size());
    }

    @Test
    void anotherPayloadUnderAQueuedIdIsAnError() {
        submit(task("t", "payload"));

        final CompletionException error = assertThrows(CompletionException.class, () -> submit(task("t", "other")));
        assertInstanceOf(IllegalArgumentException.class, error.getCause());
        assertEquals(1, assigned.size());
    }

    @Test
    void idIsFreeAgainOnceTheTaskIsFinished() {
        submit(task("t", "payload"));
        queue.complete(AttemptResult.done(assigned.getFirst(), Blob.of(new byte[0])));

        submit(task("t", "other"));
        assertEquals(2, assigned.size());
    }

    @Test
    void finalResultGoesToTheCallbacks() {
        final List<TaskResult> first = new ArrayList<>();
        final List<TaskResult> second = new ArrayList<>();
        queue.onResult(result -> {
            first.add(result);
            return CompletableFuture.completedFuture(null);
        });
        queue.onResult(result -> {
            second.add(result);
            throw new IllegalStateException("a failing callback does not stop the others");
        });
        submit(task("t", "payload"));
        queue.complete(AttemptResult.retry(assigned.getFirst(), "busy"));
        assertEquals(List.of(), first);

        queue.complete(AttemptResult.done(assigned.get(1), Blob.of(new byte[0])));

        assertEquals(1, first.size());
        assertEquals(TaskStatus.DONE, first.getFirst().status());
        assertEquals("2", first.getFirst().meta().get(TaskQueue.ATTEMPTS));
        assertEquals(first, second);
        assertTrue(results.await("t", Duration.ZERO).toCompletableFuture().join().isPresent());
    }

    /**
     * The queue serializes by the partition key only; the affinity key is about where attempts run.
     */
    @Test
    void onlyThePartitionKeySerializesTasks() {
        submit(new Task("p1", Blob.of(new byte[0]), Map.of(TaskQueue.PARTITION_KEY, "a.com")));
        submit(new Task("p2", Blob.of(new byte[0]), Map.of(TaskQueue.PARTITION_KEY, "a.com")));
        submit(new Task("w1", Blob.of(new byte[0]), Map.of(Worker.AFFINITY_KEY, "b.com")));
        submit(new Task("w2", Blob.of(new byte[0]), Map.of(Worker.AFFINITY_KEY, "b.com")));

        assertEquals(List.of("p1", "w1", "w2"), assigned.stream().map(a -> a.task().id()).toList());
    }

    /**
     * The queue is usable without a worker: a client takes the ready attempts and completes them itself.
     */
    @Test
    void nextTaskOfAKeyWaitsForThePauseWhileOtherKeysDoNot() throws InterruptedException {
        try (MemoryTaskQueue paused = new MemoryTaskQueue(new QueueSettings(3, Duration.ofMinutes(1)), DelayPolicy.fixed(PAUSE), results)) {
            final BlockingQueue<Attempt> ready = new LinkedBlockingQueue<>();
            paused.onReady(ready::add);
            paused.submit(new Task("a1", Blob.of(new byte[0]), Map.of(TaskQueue.PARTITION_KEY, "a.com")));
            paused.submit(new Task("a2", Blob.of(new byte[0]), Map.of(TaskQueue.PARTITION_KEY, "a.com")));
            paused.complete(AttemptResult.done(ready.take(), Blob.of(new byte[0])));
            final long finished = System.nanoTime();

            paused.submit(new Task("b1", Blob.of(new byte[0]), Map.of(TaskQueue.PARTITION_KEY, "b.com")));
            paused.submit(new Task("n1", Blob.of(new byte[0]), Map.of()));
            assertEquals(List.of("b1", "n1"), List.of(ready.take().task().id(), ready.take().task().id()));

            final Attempt next = ready.poll(5, TimeUnit.SECONDS);
            final long waited = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - finished);
            assertEquals("a2", next.task().id());
            assertTrue(waited >= PAUSE.toMillis(), "a2 after " + waited + " ms");
        }
    }

    /**
     * The worker never answers (its node is gone): after the attempt timeout the attempt is handed out again.
     */
    @Test
    void attemptWithoutAResultIsHandedOutAgainAfterTheTimeout() throws InterruptedException {
        try (MemoryTaskQueue timed = new MemoryTaskQueue(new QueueSettings(3, Duration.ofMillis(200)), DelayPolicy.NONE, results)) {
            final BlockingQueue<Attempt> ready = new LinkedBlockingQueue<>();
            timed.onReady(ready::add);
            timed.submit(task("t", "payload"));

            assertEquals(1, ready.take().number());
            final Attempt second = ready.poll(5, TimeUnit.SECONDS);
            assertEquals(2, second.number());
        }
    }

    private void submit(Task task) {
        queue.submit(task).toCompletableFuture().join();
    }

    private static Task task(String id, String payload) {
        return new Task(id, Blob.of(payload.getBytes(StandardCharsets.UTF_8)), Map.of());
    }
}
