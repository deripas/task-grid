package io.github.deripas.taskgrid.queue.memory;

import io.github.deripas.taskgrid.queue.QueueSettings;
import io.github.deripas.taskgrid.queue.TaskQueue;
import io.github.deripas.taskgrid.result.ResultStore;
import io.github.deripas.taskgrid.task.Attempt;
import io.github.deripas.taskgrid.task.AttemptResult;
import io.github.deripas.taskgrid.task.Task;
import io.github.deripas.taskgrid.task.TaskResult;
import io.github.deripas.taskgrid.worker.Assignment;
import io.github.deripas.taskgrid.worker.AttemptCallback;
import io.github.deripas.taskgrid.worker.Worker;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.Function;

/**
 * Task queue of a single node, in memory: a {@link Partition} per affinity key, all of them under one lock.
 * A task without an affinity key gets a partition of its own. Queued tasks are lost when the process stops.
 */
@Slf4j
public final class MemoryTaskQueue implements TaskQueue, AttemptCallback {
    private final Object lock = new Object();
    private final QueueSettings settings;
    private final ResultStore results;
    private final Worker worker;
    private final Map<String, Partition> partitions = new HashMap<>();
    private final Map<String, Queued> queuedById = new HashMap<>();

    /**
     * @param worker creates the worker for this queue: the queue is the callback the worker reports results to
     */
    public MemoryTaskQueue(
        QueueSettings settings,
        ResultStore results,
        Function<AttemptCallback, Worker> worker
    ) {
        this.settings = settings;
        this.results = results;
        this.worker = worker.apply(this);
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
            final String affinityKey = task.meta().get(AFFINITY_KEY);
            final String key = affinityKey != null
                ? affinityKey
                : UUID.randomUUID().toString();
            queuedById.put(task.id(), new Queued(key, hash));
            final Partition partition = partitions.computeIfAbsent(key, k -> new Partition(settings, new Engine(k)));
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

    private void assigned(
        String key,
        Attempt attempt,
        Assignment assignment
    ) {
        synchronized (lock) {
            final Partition partition = partitions.get(key);
            if (partition == null) {
                return;
            }
            partition.assigned(attempt, assignment);
            dropIfIdle(key, partition);
        }
    }

    private void dropIfIdle(String key, Partition partition) {
        if (partition.isIdle()) {
            partitions.remove(key, partition);
        }
    }

    /**
     * Carries out the actions of one partition; called under the lock.
     */
    @RequiredArgsConstructor
    private final class Engine implements Partition.Output {
        private final String key;

        @Override
        public void assign(Attempt attempt) {
            worker.assign(attempt).whenComplete((assignment, error) -> assigned(key, attempt, error == null
                ? assignment
                : new Assignment.Rejected("assign failed: " + error)));
        }

        @Override
        public void finished(TaskResult result) {
            queuedById.remove(result.taskId());
            results.put(result).whenComplete((ignored, error) -> {
                if (error != null) {
                    log.error("Failed to store the result: task={}", result.taskId(), error);
                }
            });
        }
    }

    /**
     * A task in the queue: its partition key and payload hash.
     */
    private record Queued(String key, String hash) {
    }
}
