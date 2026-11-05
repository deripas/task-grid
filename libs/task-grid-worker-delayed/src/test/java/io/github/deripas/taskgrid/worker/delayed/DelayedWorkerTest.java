package io.github.deripas.taskgrid.worker.delayed;

import io.github.deripas.taskgrid.queue.QueueSettings;
import io.github.deripas.taskgrid.queue.TaskQueue;
import io.github.deripas.taskgrid.queue.memory.MemoryTaskQueue;
import io.github.deripas.taskgrid.result.ResultStore;
import io.github.deripas.taskgrid.result.memory.MemoryResultStore;
import io.github.deripas.taskgrid.task.Attempt;
import io.github.deripas.taskgrid.task.Blob;
import io.github.deripas.taskgrid.task.Task;
import io.github.deripas.taskgrid.task.TaskResult;
import io.github.deripas.taskgrid.task.TaskStatus;
import io.github.deripas.taskgrid.worker.Worker;
import io.github.deripas.taskgrid.worker.local.Handler;
import io.github.deripas.taskgrid.worker.local.LocalWorker;
import io.github.deripas.taskgrid.worker.local.RetryableException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.time.Clock;
import java.time.Duration;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * DelayedWorker between the in-memory queue and a local worker; time is real, pauses are short.
 */
class DelayedWorkerTest {
    private static final Duration DELAY = Duration.ofMillis(200);
    private static final Duration WAIT = Duration.ofSeconds(5);
    private static final long TOLERANCE_MS = 10;

    private final ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor();
    private final ResultStore results = new MemoryResultStore(Duration.ofMinutes(1), Clock.systemUTC());
    /**
     * Task id + attempt number -> when the handler started it, ms since the start of the test.
     */
    private final Map<String, Long> started = new ConcurrentHashMap<>();
    private final long origin = System.nanoTime();
    private DelayedWorker delayed;
    private TaskQueue queue;

    @AfterEach
    void stop() {
        delayed.close();
        executor.shutdownNow();
    }

    @Test
    void nextAttemptOfAKeyWaitsForThePauseAfterTheResult() {
        start(attempt -> sleep(50));
        submit("a1", "a.com");
        submit("a2", "a.com");
        submit("a3", "a.com");

        awaitDone("a1", "a2", "a3");

        // each starts not earlier than the end of the previous one (50 ms) plus the pause
        assertTrue(gap("a1#1", "a2#1") >= 50 + DELAY.toMillis() - TOLERANCE_MS, "a2 after " + gap("a1#1", "a2#1"));
        assertTrue(gap("a2#1", "a3#1") >= 50 + DELAY.toMillis() - TOLERANCE_MS, "a3 after " + gap("a2#1", "a3#1"));
    }

    @Test
    void otherKeysAndTasksWithoutAKeyAreNotHeldBack() {
        start(attempt -> sleep(10));
        submit("a1", "a.com");
        submit("a2", "a.com");
        submit("b1", "b.com");
        submit("n1", null);
        submit("n2", null);

        awaitDone("a1", "a2", "b1", "n1", "n2");

        assertTrue(started.get("b1#1") < DELAY.toMillis(), "b1 at " + started.get("b1#1"));
        assertTrue(started.get("n2#1") < DELAY.toMillis(), "n2 at " + started.get("n2#1"));
        assertTrue(started.get("a2#1") >= DELAY.toMillis(), "a2 at " + started.get("a2#1"));
    }

    @Test
    void retryWaitsForThePauseToo() {
        start(attempt -> {
            if (attempt.number() == 1) {
                throw new RetryableException("site is busy");
            }
        });
        submit("a1", "a.com");

        final TaskResult result = awaitDone("a1").getFirst();

        assertEquals("2", result.meta().get(TaskQueue.ATTEMPTS));
        assertTrue(gap("a1#1", "a1#2") >= DELAY.toMillis() - TOLERANCE_MS, "retry after " + gap("a1#1", "a1#2"));
    }

    /**
     * Rejected - no handler for the kind: nothing was sent to the site, the next task of the key goes at once.
     */
    @Test
    void rejectedAttemptLeavesNoPause() {
        start(attempt -> {
        });
        queue.submit(new Task("x1", Blob.of(new byte[0]), Map.of(Worker.KIND, "unknown", Task.AFFINITY_KEY, "a.com")));
        submit("a1", "a.com");

        final TaskResult rejected = results.await("x1", WAIT).toCompletableFuture().join().orElseThrow();
        awaitDone("a1");

        assertEquals(TaskStatus.FAILED, rejected.status());
        assertTrue(started.get("a1#1") < DELAY.toMillis(), "a1 at " + started.get("a1#1"));
    }

    private void start(Step step) {
        final Handler handler = attempt -> {
            started.put(attempt.task().id() + "#" + attempt.number(), (System.nanoTime() - origin) / 1_000_000);
            step.run(attempt);
            return Blob.of(new byte[0]);
        };
        queue = new MemoryTaskQueue(new QueueSettings(3), results, callback -> {
            delayed = new DelayedWorker(new FixedDelay(DELAY), callback, inner ->
                new LocalWorker(Map.of("fetch", handler), executor, inner));
            return delayed;
        });
    }

    private void submit(String id, String key) {
        final Map<String, String> meta = new HashMap<>(Map.of(Worker.KIND, "fetch"));
        if (key != null) {
            meta.put(Task.AFFINITY_KEY, key);
        }
        queue.submit(new Task(id, Blob.of(new byte[0]), meta)).toCompletableFuture().join();
    }

    private List<TaskResult> awaitDone(String... ids) {
        return Arrays.stream(ids).map(id -> {
            final TaskResult result = results.await(id, WAIT).toCompletableFuture().join()
                .orElseThrow(() -> new AssertionError("no result for " + id));
            assertEquals(TaskStatus.DONE, result.status(), result.error());
            return result;
        }).toList();
    }

    private long gap(String first, String second) {
        return started.get(second) - started.get(first);
    }

    private static void sleep(long ms) throws InterruptedException {
        Thread.sleep(ms);
    }

    /**
     * What the handler does with an attempt.
     */
    private interface Step {
        void run(Attempt attempt) throws Exception;
    }
}
