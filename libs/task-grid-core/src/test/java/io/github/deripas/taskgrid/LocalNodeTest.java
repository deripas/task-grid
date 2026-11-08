package io.github.deripas.taskgrid;

import io.github.deripas.taskgrid.dispatch.Dispatcher;
import io.github.deripas.taskgrid.queue.DelayPolicy;
import io.github.deripas.taskgrid.queue.QueueSettings;
import io.github.deripas.taskgrid.queue.TaskQueue;
import io.github.deripas.taskgrid.queue.memory.MemoryTaskQueue;
import io.github.deripas.taskgrid.task.Attempt;
import io.github.deripas.taskgrid.task.Blob;
import io.github.deripas.taskgrid.task.Request;
import io.github.deripas.taskgrid.task.RequestId;
import io.github.deripas.taskgrid.task.Task;
import io.github.deripas.taskgrid.task.TaskResult;
import io.github.deripas.taskgrid.task.TaskStatus;
import io.github.deripas.taskgrid.worker.Worker;
import io.github.deripas.taskgrid.worker.local.Handler;
import io.github.deripas.taskgrid.worker.local.LocalWorker;
import io.github.deripas.taskgrid.worker.local.RetryableException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The whole flow in one JVM from in-memory implementations: queue, dispatcher, in-process worker, handlers.
 */
class LocalNodeTest {
    private static final Duration WAIT = Duration.ofSeconds(10);

    private final ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor();
    private final Map<String, CompletableFuture<TaskResult>> results = new ConcurrentHashMap<>();
    private final Map<String, AtomicInteger> runningByKey = new ConcurrentHashMap<>();
    private final Map<String, Queue<String>> orderByKey = new ConcurrentHashMap<>();
    private final AtomicInteger running = new AtomicInteger();
    private final AtomicInteger maxRunning = new AtomicInteger();
    private final AtomicInteger maxRunningPerKey = new AtomicInteger();
    private final Map<String, AtomicInteger> flakyCalls = new ConcurrentHashMap<>();

    private final Map<String, Handler> handlers = Map.of(
        "echo", this::echo,
        "flaky", attempt -> {
            if (flakyCalls.computeIfAbsent(name(attempt), id -> new AtomicInteger()).incrementAndGet() < 3) {
                throw new RetryableException("connection reset");
            }
            return attempt.task().payload();
        });
    private final MemoryTaskQueue queue = new MemoryTaskQueue(new QueueSettings(3, Duration.ofMinutes(1)), DelayPolicy.NONE);

    @BeforeEach
    void connect() {
        queue.onResult((result, requests) -> {
            requests.forEach(request -> result(request.id().id()).complete(result));
            return CompletableFuture.completedFuture(null);
        });
        Dispatcher.connect(queue, new LocalWorker(handlers, executor));
    }

    @AfterEach
    void stop() {
        executor.close();
        queue.close();
    }

    @Test
    void tasksOfAKeyRunOneAtATimeInOrderWhileKeysRunInParallel() {
        final List<String> ids = new ArrayList<>();
        final Map<String, List<String>> submittedByKey = new HashMap<>();
        for (int i = 0; i < 200; i++) {
            final String key = "key-" + (i % 10);
            final String id = "task-" + i;
            ids.add(id);
            submittedByKey.computeIfAbsent(key, k -> new ArrayList<>()).add(id);
            submit(id, Map.of(Worker.KIND, "echo", TaskQueue.PARTITION_KEY, key));
        }

        for (String id : ids) {
            final TaskResult result = await(id);
            assertEquals(TaskStatus.DONE, result.status());
            assertEquals(id, new String(((Blob.Inline) result.body()).bytes(), StandardCharsets.UTF_8));
        }
        assertEquals(1, maxRunningPerKey.get());
        assertTrue(maxRunning.get() > 1, "keys should run in parallel");
        submittedByKey.forEach((key, submitted) -> assertEquals(submitted, List.copyOf(orderByKey.get(key))));
    }

    @Test
    void tasksWithoutPartitionKeyRunInParallel() {
        for (int i = 0; i < 50; i++) {
            submit("task-" + i, Map.of(Worker.KIND, "echo"));
        }
        for (int i = 0; i < 50; i++) {
            assertEquals(TaskStatus.DONE, await("task-" + i).status());
        }
        assertTrue(maxRunning.get() > 1);
    }

    @Test
    void retryableFailuresAreRetried() {
        submit("t", Map.of(Worker.KIND, "flaky"));

        final TaskResult result = await("t");
        assertEquals(TaskStatus.DONE, result.status());
        assertEquals("3", result.meta().get(TaskQueue.ATTEMPTS));
    }

    @Test
    void taskOfUnknownKindFails() {
        submit("t", Map.of(Worker.KIND, "render", "client.correlation-id", "c-1"));

        final TaskResult result = await("t");
        assertEquals(TaskStatus.FAILED, result.status());
        assertEquals("rejected: no handler for kind render", result.error());
        assertEquals("c-1", result.meta().get("client.correlation-id"));
    }

    private Blob echo(Attempt attempt) throws InterruptedException {
        final String key = attempt.task().meta().getOrDefault(TaskQueue.PARTITION_KEY, name(attempt));
        final AtomicInteger perKey = runningByKey.computeIfAbsent(key, k -> new AtomicInteger());
        maxRunningPerKey.accumulateAndGet(perKey.incrementAndGet(), Math::max);
        maxRunning.accumulateAndGet(running.incrementAndGet(), Math::max);
        orderByKey.computeIfAbsent(key, k -> new ConcurrentLinkedQueue<>()).add(name(attempt));
        try {
            Thread.sleep(5);
            return attempt.task().payload();
        } finally {
            running.decrementAndGet();
            perKey.decrementAndGet();
        }
    }

    /**
     * The id of the request a task was submitted with is its payload (the id of the task is the hash of it).
     */
    private static String name(Attempt attempt) {
        return new String(((Blob.Inline) attempt.task().payload()).bytes(), StandardCharsets.UTF_8);
    }

    private void submit(String id, Map<String, String> meta) {
        final Request request = Request.of(new RequestId("test", "0", id));
        queue.submit(Task.of(Blob.of(id.getBytes(StandardCharsets.UTF_8)), meta), request).toCompletableFuture().join();
    }

    private TaskResult await(String id) {
        return result(id).orTimeout(WAIT.toMillis(), TimeUnit.MILLISECONDS).join();
    }

    private CompletableFuture<TaskResult> result(String requestId) {
        return results.computeIfAbsent(requestId, id -> new CompletableFuture<>());
    }
}
