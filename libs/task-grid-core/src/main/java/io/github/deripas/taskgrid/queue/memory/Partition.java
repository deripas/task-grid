package io.github.deripas.taskgrid.queue.memory;

import io.github.deripas.taskgrid.queue.DelayPolicy;
import io.github.deripas.taskgrid.queue.QueueSettings;
import io.github.deripas.taskgrid.queue.TaskQueue;
import io.github.deripas.taskgrid.task.Attempt;
import io.github.deripas.taskgrid.task.AttemptResult;
import io.github.deripas.taskgrid.task.Blob;
import io.github.deripas.taskgrid.task.Task;
import io.github.deripas.taskgrid.task.TaskResult;
import io.github.deripas.taskgrid.task.TaskStatus;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.time.Duration;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.HashMap;
import java.util.Map;

/**
 * Partition of the queue: the tasks of one partition key, in order, one attempt in progress at a time, retries by
 * attempt status, a pause after each result ({@link DelayPolicy}) before the next attempt - a retry or the next task.
 * <p>
 * Pure state machine: no threads, no time. Methods are called one at a time by an engine (in-memory, actor),
 * which carries out the {@link Output} actions: handing attempts out, storing final results, waking the partition
 * up with {@link #resume} when a pause is over.
 * <p>
 * The {@link Output#assign} call is the last action of a method: the engine may report a result of the attempt from
 * inside it.
 */
@Slf4j
@RequiredArgsConstructor
final class Partition {
    /**
     * Actions the partition delegates to its engine.
     */
    public interface Output {
        /**
         * Hand the attempt out; its result goes to {@link #completed}.
         */
        void assign(Attempt attempt);

        /**
         * The task is finished: store its result.
         */
        void finished(TaskResult result);

        /**
         * Call {@link #resume} after the delay.
         */
        void resumeAfter(Duration delay);
    }

    /**
     * {@code null} - a partition of one independent task: no pauses.
     */
    private final String key;
    private final QueueSettings settings;
    private final DelayPolicy delay;
    private final Output out;
    private final Deque<Task> waiting = new ArrayDeque<>();
    /**
     * The attempt in progress; during a pause - the retry waiting for it, not handed out yet.
     */
    private Attempt current;
    private boolean paused;

    public void submit(Task task) {
        waiting.addLast(task);
        if (current == null && !paused) {
            startNext();
        }
    }

    /**
     * Result of the attempt in progress. A result of an attempt that is not in progress (late, duplicate) is ignored.
     */
    public void completed(AttemptResult result) {
        if (paused || current == null || !current.task().id().equals(result.taskId()) || current.number() != result.attempt()) {
            log.debug("Ignoring result of an attempt not in progress: task={} attempt={}", result.taskId(), result.attempt());
            return;
        }
        final Duration pause = key == null
            ? Duration.ZERO
            : delay.after(key, result);
        // before the result goes out: a task submitted from a result callback must see the pause
        paused = pause.isPositive();
        final boolean retrying = switch (result.status()) {
            case DONE -> finish(TaskStatus.DONE, result.body(), null);
            case FAILED -> finish(TaskStatus.FAILED, null, result.error());
            case RETRY -> retry(result.error());
        };
        if (paused) {
            out.resumeAfter(pause);
        } else if (retrying) {
            out.assign(current);
        } else if (current == null) {
            startNext();
        }
    }

    /**
     * The pause is over: the retry or the next task goes out.
     */
    public void resume() {
        if (!paused) {
            return;
        }
        paused = false;
        if (current != null) {
            out.assign(current);
        } else {
            startNext();
        }
    }

    /**
     * Nothing waiting, nothing in progress, no pause: the engine may drop the partition.
     */
    public boolean isIdle() {
        return current == null && waiting.isEmpty() && !paused;
    }

    /**
     * {@code true} - the next attempt is the {@link #current} one, not handed out yet.
     */
    private boolean retry(String error) {
        if (current.number() >= settings.maxAttempts()) {
            return finish(TaskStatus.FAILED, null, "attempts exhausted, last error: " + error);
        }
        current = new Attempt(current.task(), current.number() + 1);
        return true;
    }

    private boolean finish(
        TaskStatus status,
        Blob body,
        String error
    ) {
        final Attempt last = current;
        final Map<String, String> meta = new HashMap<>(last.task().meta());
        meta.put(TaskQueue.ATTEMPTS, String.valueOf(last.number()));
        current = null;
        out.finished(new TaskResult(last.task().id(), status, body, error, meta));
        return false;
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
