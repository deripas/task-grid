package io.github.deripas.taskgrid.queue.partition;

import io.github.deripas.taskgrid.queue.DelayPolicy;
import io.github.deripas.taskgrid.queue.QueueSettings;
import io.github.deripas.taskgrid.queue.TaskQueue;
import io.github.deripas.taskgrid.queue.partition.PartitionEvent.Delivered;
import io.github.deripas.taskgrid.queue.partition.PartitionEvent.Ended;
import io.github.deripas.taskgrid.queue.partition.PartitionEvent.Started;
import io.github.deripas.taskgrid.queue.partition.PartitionEvent.Submitted;
import io.github.deripas.taskgrid.task.Attempt;
import io.github.deripas.taskgrid.task.AttemptResult;
import io.github.deripas.taskgrid.task.AttemptStatus;
import io.github.deripas.taskgrid.task.Blob;
import io.github.deripas.taskgrid.task.Task;
import io.github.deripas.taskgrid.task.TaskResult;
import io.github.deripas.taskgrid.task.TaskStatus;
import lombok.extern.slf4j.Slf4j;

import java.time.Duration;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Deque;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Partition of a queue: the tasks of one partition key, in order, one attempt in progress at a time, retries by
 * attempt status, a pause after each result ({@link DelayPolicy}) before the next attempt - a retry or the next task,
 * a deadline of the attempt in progress ({@link QueueSettings#attemptTimeout}), final results kept until delivered.
 * <p>
 * Event-sourced state machine, the same for every engine (in-memory, Pekko). A command ({@link #submit},
 * {@link #completed}, {@link #tick}, {@link #delivered}) only decides: it returns the events and does not change
 * the state; {@link #apply} changes it - right away in memory, after the events are persisted in Pekko, and on replay.
 * The engine carries out the effects of the applied events: {@link Started} - hand the attempt out, {@link Ended}
 * with a result - deliver it, then {@link #delivered}; and calls {@link #tick} at {@link #nextDeadline}.
 * <p>
 * No threads, no clock: time is a parameter (epoch ms). Methods are called one at a time.
 */
@Slf4j
public final class Partition {
    /**
     * {@code null} - a partition of one independent task: no pauses.
     */
    private final String key;
    private final QueueSettings settings;
    private final DelayPolicy delay;
    private final Deque<Task> waiting = new ArrayDeque<>();
    /**
     * Task id -> payload hash, of the tasks waiting and in progress.
     */
    private final Map<String, String> queued = new HashMap<>();
    private final Map<String, TaskResult> undelivered = new LinkedHashMap<>();
    /**
     * The attempt in progress ({@link #running}) or the retry waiting for the pause.
     */
    private Attempt current;
    private boolean running;
    private long deadline;
    private long resumeAt;

    public Partition(
        String key,
        QueueSettings settings,
        DelayPolicy delay
    ) {
        this.key = key;
        this.settings = settings;
        this.delay = delay;
    }

    /**
     * The partition as it was at the snapshot.
     */
    public static Partition restore(
        String key,
        QueueSettings settings,
        DelayPolicy delay,
        PartitionSnapshot snapshot
    ) {
        final Partition partition = new Partition(key, settings, delay);
        snapshot.waiting().forEach(task -> {
            partition.waiting.addLast(task);
            partition.queued.put(task.id(), task.payload().hash());
        });
        if (snapshot.current() != null) {
            partition.current = snapshot.current();
            partition.queued.put(snapshot.current().task().id(), snapshot.current().task().payload().hash());
        }
        partition.running = snapshot.running();
        partition.deadline = snapshot.deadline();
        partition.resumeAt = snapshot.resumeAt();
        snapshot.undelivered().forEach(result -> partition.undelivered.put(result.taskId(), result));
        return partition;
    }

    /**
     * A task whose id is in the partition is not queued again: the same payload (by hash) is a retried submission -
     * no events; another payload is an {@link IllegalArgumentException}.
     */
    public List<PartitionEvent> submit(Task task, long now) {
        final String hash = queued.get(task.id());
        if (hash != null) {
            if (hash.equals(task.payload().hash())) {
                return List.of();
            }
            throw new IllegalArgumentException("task id is taken by another payload: " + task.id());
        }
        final List<PartitionEvent> events = new ArrayList<>(2);
        events.add(new Submitted(task));
        if (!running && current == null && now >= resumeAt) {
            final Task head = waiting.isEmpty()
                ? task
                : waiting.peekFirst();
            events.add(start(new Attempt(head, 1), now));
        }
        return events;
    }

    /**
     * Result of the attempt in progress; a result of an attempt that is not in progress (late, duplicate, timed out)
     * gives no events.
     */
    public List<PartitionEvent> completed(AttemptResult result, long now) {
        if (!running || !current.task().id().equals(result.taskId()) || current.number() != result.attempt()) {
            log.debug("Ignoring result of an attempt not in progress: task={} attempt={}", result.taskId(), result.attempt());
            return List.of();
        }
        return end(result, now);
    }

    /**
     * Time passed: the attempt in progress is over its deadline (RETRY), or the pause is over (the next attempt).
     */
    public List<PartitionEvent> tick(long now) {
        if (running) {
            return now >= deadline
                ? end(timedOut(), now)
                : List.of();
        }
        if (now < resumeAt) {
            return List.of();
        }
        final Attempt next = current != null
            ? current
            : waiting.isEmpty()
                ? null
                : new Attempt(waiting.peekFirst(), 1);
        return next == null
            ? List.of()
            : List.of(start(next, now));
    }

    /**
     * The final result of the task reached its receivers.
     */
    public List<PartitionEvent> delivered(String taskId) {
        return undelivered.containsKey(taskId)
            ? List.of(new Delivered(taskId))
            : List.of();
    }

    public void apply(PartitionEvent event) {
        switch (event) {
            case Submitted submitted -> {
                waiting.addLast(submitted.task());
                queued.put(submitted.task().id(), submitted.task().payload().hash());
            }
            case Started started -> {
                if (started.attempt().number() == 1) {
                    waiting.removeFirst();
                }
                current = started.attempt();
                running = true;
                deadline = started.deadline();
            }
            case Ended ended -> {
                running = false;
                deadline = 0;
                resumeAt = ended.resumeAt();
                if (ended.result() == null) {
                    current = new Attempt(current.task(), ended.attempt() + 1);
                } else {
                    current = null;
                    queued.remove(ended.taskId());
                    undelivered.put(ended.taskId(), ended.result());
                }
            }
            case Delivered delivered -> undelivered.remove(delivered.taskId());
        }
    }

    /**
     * When {@link #tick} has something to do (epoch ms): the deadline of the attempt in progress, or the end of the
     * pause; at or before {@code now} - nothing to wait for.
     */
    public long nextDeadline() {
        return running
            ? deadline
            : resumeAt;
    }

    public PartitionSnapshot snapshot() {
        return new PartitionSnapshot(List.copyOf(waiting), current, running, deadline, resumeAt, List.copyOf(undelivered.values()));
    }

    /**
     * Final results not delivered yet: to deliver again after a restart.
     */
    public Collection<TaskResult> undelivered() {
        return List.copyOf(undelivered.values());
    }

    /**
     * Nothing waiting, nothing in progress, nothing to deliver; the pause may still last.
     */
    public boolean isEmpty() {
        return !running && current == null && waiting.isEmpty() && undelivered.isEmpty();
    }

    /**
     * Empty and the pause is over: the engine may drop the partition - a task of the key coming later may start at once.
     */
    public boolean isIdle(long now) {
        return isEmpty() && now >= resumeAt;
    }

    private Started start(Attempt attempt, long now) {
        return new Started(attempt, now + settings.attemptTimeout().toMillis());
    }

    /**
     * The attempt in progress ended with the result: the task finishes or retries, the pause starts; without a pause
     * the next attempt goes out at once.
     */
    private List<PartitionEvent> end(AttemptResult result, long now) {
        final Duration pause = key == null
            ? Duration.ZERO
            : delay.after(key, result);
        final long resume = pause.isPositive()
            ? now + pause.toMillis()
            : now;
        final TaskResult finished = switch (result.status()) {
            case DONE -> finish(TaskStatus.DONE, result.body(), null);
            case FAILED -> finish(TaskStatus.FAILED, null, result.error());
            case RETRY -> current.number() >= settings.maxAttempts()
                ? finish(TaskStatus.FAILED, null, "attempts exhausted, last error: " + result.error())
                : null;
        };
        final List<PartitionEvent> events = new ArrayList<>(2);
        events.add(new Ended(current.task().id(), current.number(), finished, resume));
        if (resume <= now) {
            if (finished == null) {
                events.add(start(new Attempt(current.task(), current.number() + 1), now));
            } else if (!waiting.isEmpty()) {
                events.add(start(new Attempt(waiting.peekFirst(), 1), now));
            }
        }
        return events;
    }

    private TaskResult finish(
        TaskStatus status,
        Blob body,
        String error
    ) {
        final Map<String, String> meta = new HashMap<>(current.task().meta());
        meta.put(TaskQueue.ATTEMPTS, String.valueOf(current.number()));
        return new TaskResult(current.task().id(), status, body, error, meta);
    }

    private AttemptResult timedOut() {
        return new AttemptResult(current.task().id(), current.number(), current.task().meta(), AttemptStatus.RETRY, null,
            "no result within " + settings.attemptTimeout());
    }
}
