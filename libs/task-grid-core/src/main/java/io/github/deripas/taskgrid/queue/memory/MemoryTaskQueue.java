package io.github.deripas.taskgrid.queue.memory;

import io.github.deripas.taskgrid.queue.DelayPolicy;
import io.github.deripas.taskgrid.queue.QueueSettings;
import io.github.deripas.taskgrid.queue.ReadyCallback;
import io.github.deripas.taskgrid.queue.ResultCallback;
import io.github.deripas.taskgrid.queue.TaskQueue;
import io.github.deripas.taskgrid.result.ResultStore;
import io.github.deripas.taskgrid.task.Attempt;
import io.github.deripas.taskgrid.task.AttemptResult;
import io.github.deripas.taskgrid.task.Task;
import io.github.deripas.taskgrid.task.TaskResult;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Task queue of a single node, in memory: a {@link Partition} per partition key, all of them under one lock.
 * A task without a partition key gets a partition of its own, without pauses. Pauses end on a timer of its own thread.
 * Queued tasks are lost when the process stops.
 */
@Slf4j
public final class MemoryTaskQueue implements TaskQueue, AutoCloseable {
    private final Object lock = new Object();
    private final QueueSettings settings;
    private final DelayPolicy delay;
    private final ResultStore results;
    private final ScheduledExecutorService timer = Executors.newSingleThreadScheduledExecutor(
        Thread.ofPlatform().name("memory-task-queue").daemon().factory());
    private final Map<String, Partition> partitions = new HashMap<>();
    private final Map<String, Queued> queuedById = new HashMap<>();
    private final List<ReadyCallback> ready = new CopyOnWriteArrayList<>();
    private final List<ResultCallback> callbacks = new CopyOnWriteArrayList<>();

    public MemoryTaskQueue(
        QueueSettings settings,
        DelayPolicy delay,
        ResultStore results
    ) {
        this.settings = settings;
        this.delay = delay;
        this.results = results;
    }

    /**
     * A task whose id is still in the queue is not queued again: the same payload (by hash) is a retried submission
     * and succeeds, another payload fails with {@link IllegalArgumentException}.
     */
    @Override
    public CompletionStage<Void> submit(Task task) {
        final String hash = task.payload().hash();
        synchronized (lock) {
            final Queued queued = queuedById.get(task.id());
            if (queued != null) {
                return queued.hash().equals(hash)
                    ? CompletableFuture.completedFuture(null)
                    : CompletableFuture.failedFuture(new IllegalArgumentException("task id is taken by another payload: " + task.id()));
            }
            final String partitionKey = task.meta().get(PARTITION_KEY);
            final String key = partitionKey != null
                ? partitionKey
                : UUID.randomUUID().toString();
            queuedById.put(task.id(), new Queued(key, hash));
            final Partition partition = partitions.computeIfAbsent(key, k ->
                new Partition(partitionKey, settings, delay, new Actions(k)));
            partition.submit(task);
            dropIfIdle(key, partition);
        }
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public void complete(AttemptResult result) {
        synchronized (lock) {
            final Queued queued = queuedById.get(result.taskId());
            if (queued == null) {
                log.debug("Ignoring result of an unknown task: task={} attempt={}", result.taskId(), result.attempt());
                return;
            }
            final Partition partition = partitions.get(queued.key());
            partition.completed(result);
            dropIfIdle(queued.key(), partition);
        }
    }

    /**
     * Receivers are called under the lock of the queue.
     */
    @Override
    public void onReady(ReadyCallback callback) {
        ready.add(callback);
    }

    /**
     * Each result is delivered once; a failed stage is logged, not repeated: the queue keeps nothing.
     */
    @Override
    public void onResult(ResultCallback callback) {
        callbacks.add(callback);
    }

    /**
     * Stops the timer: partitions in a pause stay there.
     */
    @Override
    public void close() {
        timer.shutdownNow();
    }

    private void resume(String key, Partition partition) {
        synchronized (lock) {
            if (partitions.get(key) != partition) {
                return;
            }
            partition.resume();
            dropIfIdle(key, partition);
        }
    }

    /**
     * Under the lock.
     */
    private void dropIfIdle(String key, Partition partition) {
        if (partition.isIdle()) {
            partitions.remove(key, partition);
        }
    }

    /**
     * Carries out the actions of one partition; called under the lock.
     */
    @RequiredArgsConstructor
    private final class Actions implements Partition.Output {
        private final String key;

        @Override
        public void assign(Attempt attempt) {
            for (ReadyCallback callback : ready) {
                try {
                    callback.ready(attempt);
                } catch (RuntimeException e) {
                    log.error("Ready callback failed: task={} attempt={}", attempt.task().id(), attempt.number(), e);
                }
            }
        }

        @Override
        public void finished(TaskResult result) {
            queuedById.remove(result.taskId());
            results.put(result).whenComplete((ignored, error) -> {
                if (error != null) {
                    log.error("Failed to store the result: task={}", result.taskId(), error);
                }
            });
            for (ResultCallback callback : callbacks) {
                deliver(callback, result);
            }
        }

        @Override
        public void resumeAfter(Duration delay) {
            final Partition partition = partitions.get(key);
            try {
                timer.schedule(() -> resume(key, partition), delay.toNanos(), TimeUnit.NANOSECONDS);
            } catch (RejectedExecutionException e) {
                log.debug("Queue is closed, partition {} stays paused", key);
            }
        }
    }

    private static void deliver(ResultCallback callback, TaskResult result) {
        final CompletionStage<Void> delivered;
        try {
            delivered = callback.finished(result);
        } catch (RuntimeException e) {
            log.error("Result callback failed: task={}", result.taskId(), e);
            return;
        }
        delivered.whenComplete((ignored, error) -> {
            if (error != null) {
                log.error("Result callback failed: task={}", result.taskId(), error);
            }
        });
    }

    /**
     * A task in the queue: its partition key and payload hash.
     */
    private record Queued(String key, String hash) {
    }
}
