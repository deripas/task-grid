package io.github.deripas.taskgrid.queue.memory;

import io.github.deripas.taskgrid.queue.QueueSettings;
import io.github.deripas.taskgrid.queue.TaskQueue;
import io.github.deripas.taskgrid.task.Attempt;
import io.github.deripas.taskgrid.task.AttemptResult;
import io.github.deripas.taskgrid.task.Blob;
import io.github.deripas.taskgrid.task.Task;
import io.github.deripas.taskgrid.task.TaskResult;
import io.github.deripas.taskgrid.task.TaskStatus;
import io.github.deripas.taskgrid.worker.Assignment;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.HashMap;
import java.util.Map;

/**
 * Partition of the queue: the tasks of one affinity key, in order, one attempt in progress at a time, retries by attempt status.
 * <p>
 * Pure state machine: no threads, no engine. Methods are called one at a time by an engine (in-memory, actor),
 * which carries out the {@link Output} actions: assigning attempts to the worker and storing final results.
 * The engine reports the worker's answer to an assignment back through {@link #assigned}.
 */
@Slf4j
@RequiredArgsConstructor
final class Partition {
    /**
     * Actions the queue delegates to its engine.
     */
    public interface Output {
        /**
         * Hand the attempt to the worker; its answer goes to {@link #assigned}.
         */
        void assign(Attempt attempt);

        /**
         * The task is finished: store its result.
         */
        void finished(TaskResult result);
    }

    private final QueueSettings settings;
    private final Output out;
    private final Deque<Task> waiting = new ArrayDeque<>();
    private Attempt current;

    public void submit(Task task) {
        waiting.addLast(task);
        if (current == null) {
            startNext();
        }
    }

    /**
     * Answer of the worker to {@link Output#assign}. A rejected attempt fails the task: no worker can take it.
     */
    public void assigned(Attempt attempt, Assignment assignment) {
        if (!attempt.equals(current)) {
            return;
        }
        if (assignment instanceof Assignment.Rejected rejected) {
            finish(TaskStatus.FAILED, null, "rejected: " + rejected.reason());
        }
    }

    /**
     * Result of an attempt. A result of an attempt that is not in progress (late, duplicate) is ignored.
     */
    public void completed(AttemptResult result) {
        if (current == null || !current.task().id().equals(result.taskId()) || current.number() != result.attempt()) {
            log.debug("Ignoring result of an attempt not in progress: task={} attempt={}", result.taskId(), result.attempt());
            return;
        }
        switch (result.status()) {
            case DONE -> finish(TaskStatus.DONE, result.body(), null);
            case FAILED -> finish(TaskStatus.FAILED, null, result.error());
            case RETRY -> retry(result.error());
        }
    }

    /**
     * Nothing waiting and nothing in progress: the engine may drop the partition.
     */
    public boolean isIdle() {
        return current == null && waiting.isEmpty();
    }

    private void retry(String error) {
        if (current.number() >= settings.maxAttempts()) {
            finish(TaskStatus.FAILED, null, "attempts exhausted, last error: " + error);
            return;
        }
        current = new Attempt(current.task(), current.number() + 1);
        out.assign(current);
    }

    private void finish(
        TaskStatus status,
        Blob body,
        String error
    ) {
        final Attempt last = current;
        final Map<String, String> meta = new HashMap<>(last.task().meta());
        meta.put(TaskQueue.ATTEMPTS, String.valueOf(last.number()));
        current = null;
        out.finished(new TaskResult(last.task().id(), status, body, error, meta));
        if (current == null) {
            startNext();
        }
    }

    private void startNext() {
        final Task next = waiting.pollFirst();
        if (next == null) {
            return;
        }
        current = new Attempt(next, 1);
        out.assign(current);
    }
}
