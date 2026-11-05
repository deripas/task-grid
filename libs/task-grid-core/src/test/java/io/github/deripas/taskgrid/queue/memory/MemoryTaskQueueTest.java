package io.github.deripas.taskgrid.queue.memory;

import io.github.deripas.taskgrid.queue.QueueSettings;
import io.github.deripas.taskgrid.queue.TaskQueue;
import io.github.deripas.taskgrid.result.memory.MemoryResultStore;
import io.github.deripas.taskgrid.task.Attempt;
import io.github.deripas.taskgrid.task.AttemptResult;
import io.github.deripas.taskgrid.task.Blob;
import io.github.deripas.taskgrid.task.Task;
import io.github.deripas.taskgrid.task.TaskResult;
import io.github.deripas.taskgrid.task.TaskStatus;
import io.github.deripas.taskgrid.worker.Assignment;
import io.github.deripas.taskgrid.worker.AttemptCallback;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class MemoryTaskQueueTest {
    private final List<Attempt> assigned = new ArrayList<>();
    private final MemoryResultStore results = new MemoryResultStore(Duration.ofMinutes(1), Clock.systemUTC());
    /**
     * Set by the worker factory while the queue is constructed: must be declared before the queue.
     */
    private AttemptCallback callback;
    private final MemoryTaskQueue queue = new MemoryTaskQueue(new QueueSettings(3), results, callback -> {
        this.callback = callback;
        return attempt -> {
            assigned.add(attempt);
            return CompletableFuture.completedFuture(new Assignment.Accepted());
        };
    });

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
        callback.complete(AttemptResult.done(assigned.getFirst(), Blob.of(new byte[0])));

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
        callback.complete(AttemptResult.retry(assigned.getFirst(), "busy"));
        assertEquals(List.of(), first);

        callback.complete(AttemptResult.done(assigned.get(1), Blob.of(new byte[0])));

        assertEquals(1, first.size());
        assertEquals(TaskStatus.DONE, first.getFirst().status());
        assertEquals("2", first.getFirst().meta().get(TaskQueue.ATTEMPTS));
        assertEquals(first, second);
        assertTrue(results.await("t", Duration.ZERO).toCompletableFuture().join().isPresent());
    }

    private void submit(Task task) {
        queue.submit(task).toCompletableFuture().join();
    }

    private static Task task(String id, String payload) {
        return new Task(id, Blob.of(payload.getBytes(StandardCharsets.UTF_8)), Map.of());
    }
}
