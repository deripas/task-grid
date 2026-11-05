package io.github.deripas.taskgrid.queue.memory;

import io.github.deripas.taskgrid.queue.DelayPolicy;
import io.github.deripas.taskgrid.queue.QueueSettings;
import io.github.deripas.taskgrid.queue.ReadyCallback;
import io.github.deripas.taskgrid.queue.ResultCallback;
import io.github.deripas.taskgrid.queue.TaskQueue;
import io.github.deripas.taskgrid.queue.partition.Partition;
import io.github.deripas.taskgrid.queue.partition.PartitionEvent;
import io.github.deripas.taskgrid.result.ResultStore;
import io.github.deripas.taskgrid.task.Attempt;
import io.github.deripas.taskgrid.task.AttemptResult;
import io.github.deripas.taskgrid.task.Task;
import io.github.deripas.taskgrid.task.TaskResult;
import lombok.extern.slf4j.Slf4j;

import java.time.Clock;
import java.util.ArrayList;
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
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/**
 * Task queue of a single node, in memory: a {@link Partition} per partition key, all of them under one lock; events
 * are applied at once. A task without a partition key gets a partition of its own, without pauses. Pauses and
 * deadlines of attempts end on a timer of its own thread. Queued tasks are lost when the process stops; a final
 * result is delivered once.
 */
@Slf4j
public final class MemoryTaskQueue implements TaskQueue, AutoCloseable {
    private final Object lock = new Object();
    private final QueueSettings settings;
    private final DelayPolicy delay;
    private final ResultStore results;
    private final Clock clock = Clock.systemUTC();
    private final ScheduledExecutorService timer = Executors.newSingleThreadScheduledExecutor(
        Thread.ofPlatform().name("memory-task-queue").daemon().factory());
    private final Map<String, Entry> partitions = new HashMap<>();
    /**
     * Task id -> key of its partition, while the task is in the queue.
     */
    private final Map<String, String> keyById = new HashMap<>();
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
        final Effects effects;
        synchronized (lock) {
            final String partitionKey = task.meta().get(PARTITION_KEY);
            final String key = keyById.getOrDefault(task.id(), partitionKey != null
                ? partitionKey
                : UUID.randomUUID().toString());
            final Entry entry = partitions.computeIfAbsent(key, k -> new Entry(k, new Partition(partitionKey, settings, delay)));
            final List<PartitionEvent> events;
            try {
                events = entry.partition.submit(task, now());
            } catch (IllegalArgumentException e) {
                dropIfIdle(entry);
                return CompletableFuture.failedFuture(e);
            }
            keyById.put(task.id(), key);
            effects = run(entry, events);
        }
        effects.carryOut();
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public CompletionStage<Void> complete(AttemptResult result) {
        final Effects effects;
        synchronized (lock) {
            final String key = keyById.get(result.taskId());
            final Entry entry = key == null
                ? null
                : partitions.get(key);
            if (entry == null) {
                log.debug("Ignoring result of an unknown task: task={} attempt={}", result.taskId(), result.attempt());
                return CompletableFuture.completedFuture(null);
            }
            effects = run(entry, entry.partition.completed(result, now()));
        }
        effects.carryOut();
        return CompletableFuture.completedFuture(null);
    }

    /**
     * Receivers are called on the thread that submitted a task, completed an attempt, or of the timer; not under
     * the lock of the queue.
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
     * Stops the timer: pauses and deadlines of attempts no longer end.
     */
    @Override
    public void close() {
        timer.shutdownNow();
    }

    private void tick(Entry entry) {
        final Effects effects;
        synchronized (lock) {
            if (partitions.get(entry.key) != entry) {
                return;
            }
            effects = run(entry, entry.partition.tick(now()));
        }
        effects.carryOut();
    }

    /**
     * Applies the events, then collects their effects: the state is complete before anyone hears of it, so a receiver
     * may call the queue again at once. A final result counts as delivered here: the in-memory queue delivers once.
     * Under the lock.
     */
    private Effects run(Entry entry, List<PartitionEvent> events) {
        final Effects effects = new Effects();
        for (PartitionEvent event : events) {
            entry.partition.apply(event);
            switch (event) {
                case PartitionEvent.Started started -> effects.started.add(started.attempt());
                case PartitionEvent.Ended ended when ended.result() != null -> effects.finished.add(ended.result());
                default -> {
                }
            }
        }
        for (TaskResult result : effects.finished) {
            keyById.remove(result.taskId());
            entry.partition.delivered(result.taskId()).forEach(entry.partition::apply);
        }
        schedule(entry);
        dropIfIdle(entry);
        return effects;
    }

    /**
     * Under the lock.
     */
    private void schedule(Entry entry) {
        final long deadline = entry.partition.nextDeadline();
        if (entry.timer != null && entry.timerAt == deadline) {
            return;
        }
        if (entry.timer != null) {
            entry.timer.cancel(false);
            entry.timer = null;
        }
        final long wait = deadline - now();
        if (wait <= 0) {
            return;
        }
        try {
            entry.timer = timer.schedule(() -> tick(entry), wait, TimeUnit.MILLISECONDS);
            entry.timerAt = deadline;
        } catch (RejectedExecutionException e) {
            log.debug("Queue is closed, partition {} is not woken up", entry.key);
        }
    }

    /**
     * Under the lock.
     */
    private void dropIfIdle(Entry entry) {
        if (entry.partition.isIdle(now())) {
            partitions.remove(entry.key, entry);
            if (entry.timer != null) {
                entry.timer.cancel(false);
            }
        }
    }

    private long now() {
        return clock.millis();
    }

    /**
     * What to tell the receivers after the lock is released.
     */
    private final class Effects {
        private final List<Attempt> started = new ArrayList<>(1);
        private final List<TaskResult> finished = new ArrayList<>(1);

        void carryOut() {
            for (TaskResult result : finished) {
                results.put(result).whenComplete((ignored, error) -> {
                    if (error != null) {
                        log.error("Failed to store the result: task={}", result.taskId(), error);
                    }
                });
                for (ResultCallback callback : callbacks) {
                    deliver(callback, result);
                }
            }
            for (Attempt attempt : started) {
                for (ReadyCallback callback : ready) {
                    try {
                        callback.ready(attempt);
                    } catch (RuntimeException e) {
                        log.error("Ready callback failed: task={} attempt={}", attempt.task().id(), attempt.number(), e);
                    }
                }
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
     * A partition and its timer.
     */
    private static final class Entry {
        private final String key;
        private final Partition partition;
        private ScheduledFuture<?> timer;
        private long timerAt;

        Entry(String key, Partition partition) {
            this.key = key;
            this.partition = partition;
        }
    }
}
