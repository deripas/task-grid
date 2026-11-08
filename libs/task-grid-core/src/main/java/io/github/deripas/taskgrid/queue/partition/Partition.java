package io.github.deripas.taskgrid.queue.partition;

import io.github.deripas.taskgrid.queue.DelayPolicy;
import io.github.deripas.taskgrid.queue.QueueSettings;
import io.github.deripas.taskgrid.queue.TaskQueue;
import io.github.deripas.taskgrid.queue.partition.PartitionEvent.Ended;
import io.github.deripas.taskgrid.queue.partition.PartitionEvent.Joined;
import io.github.deripas.taskgrid.queue.partition.PartitionEvent.Started;
import io.github.deripas.taskgrid.queue.partition.PartitionEvent.Submitted;
import io.github.deripas.taskgrid.queue.partition.PartitionSnapshot.Work;
import io.github.deripas.taskgrid.task.Attempt;
import io.github.deripas.taskgrid.task.AttemptResult;
import io.github.deripas.taskgrid.task.AttemptStatus;
import io.github.deripas.taskgrid.task.Blob;
import io.github.deripas.taskgrid.task.Request;
import io.github.deripas.taskgrid.task.RequestId;
import io.github.deripas.taskgrid.task.Task;
import io.github.deripas.taskgrid.task.TaskResult;
import io.github.deripas.taskgrid.task.TaskStatus;
import lombok.extern.slf4j.Slf4j;

import java.time.Duration;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Partition of a queue: the tasks of one partition key, in order, one attempt in progress at a time, retries by
 * attempt status, a pause after each result ({@link DelayPolicy}) before the next attempt - a retry or the next task,
 * a deadline of the attempt in progress ({@link QueueSettings#attemptTimeout}). A task is done once for all the
 * requests that ask for it while it is here.
 * <p>
 * Event-sourced state machine, the same for every engine (in-memory, Pekko). A command ({@link #submit},
 * {@link #completed}, {@link #tick}) only decides: it returns the events (and the final result, see {@link Step}) and
 * does not change the state; {@link #apply} changes it - right away in memory, after the events are persisted in
 * Pekko, and on replay. The engine carries out the effects: delivers a final result of a step before its events,
 * hands out the attempt of an applied {@link Started} ({@link #attempt}), calls {@link #tick} at
 * {@link #nextDeadline}.
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
    /**
     * Tasks waiting and in progress, by id.
     */
    private final Map<String, Task> tasks = new HashMap<>();
    /**
     * Requests of each task in {@link #tasks}, in the order they came.
     */
    private final Map<String, List<Request>> requests = new HashMap<>();
    /**
     * Task of each request in the partition.
     */
    private final Map<RequestId, String> owners = new HashMap<>();
    private final Deque<String> waiting = new ArrayDeque<>();
    /**
     * The task in progress ({@link #running}) or retrying after the pause; {@code null} - none.
     */
    private String current;
    private int attempt;
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
        snapshot.waiting().forEach(work -> {
            partition.add(work);
            partition.waiting.addLast(work.task().id());
        });
        if (snapshot.current() != null) {
            partition.add(snapshot.current());
            partition.current = snapshot.current().task().id();
        }
        partition.attempt = snapshot.attempt();
        partition.running = snapshot.running();
        partition.deadline = snapshot.deadline();
        partition.resumeAt = snapshot.resumeAt();
        return partition;
    }

    /**
     * A task already here is not queued again: the request joins it. The same request again gives no events.
     *
     * @throws IllegalArgumentException the id of the task is here with another payload (by hash), or the request is
     *                                  here with another task
     */
    public List<PartitionEvent> submit(
        Task task,
        Request request,
        long now
    ) {
        final Task known = tasks.get(task.id());
        if (known != null && !known.payload().hash().equals(task.payload().hash())) {
            throw new IllegalArgumentException("task id is taken by another payload: " + task.id());
        }
        final String owner = owners.get(request.id());
        if (owner != null) {
            if (owner.equals(task.id())) {
                return List.of();
            }
            throw new IllegalArgumentException("request " + request.id() + " is here with another task: " + owner);
        }
        if (known != null) {
            return List.of(new Joined(task.id(), request));
        }
        final List<PartitionEvent> events = new ArrayList<>(2);
        events.add(new Submitted(task, request));
        if (!running && current == null && now >= resumeAt) {
            final String head = waiting.isEmpty()
                ? task.id()
                : waiting.peekFirst();
            events.add(start(head, 1, now));
        }
        return events;
    }

    /**
     * Result of the attempt in progress; a result of an attempt that is not in progress (late, duplicate, timed out)
     * decides nothing.
     */
    public Step completed(AttemptResult result, long now) {
        if (!running || !current.equals(result.taskId()) || attempt != result.attempt()) {
            log.debug("Ignoring result of an attempt not in progress: task={} attempt={}", result.taskId(), result.attempt());
            return Step.NONE;
        }
        return end(result, now);
    }

    /**
     * Time passed: the attempt in progress is over its deadline (RETRY), or the pause is over (the next attempt).
     */
    public Step tick(long now) {
        if (running) {
            return now >= deadline
                ? end(timedOut(), now)
                : Step.NONE;
        }
        if (now < resumeAt) {
            return Step.NONE;
        }
        if (current != null) {
            return new Step(List.of(start(current, attempt, now)), null);
        }
        return waiting.isEmpty()
            ? Step.NONE
            : new Step(List.of(start(waiting.peekFirst(), 1, now)), null);
    }

    public void apply(PartitionEvent event) {
        switch (event) {
            case Submitted submitted -> {
                add(new Work(submitted.task(), List.of(submitted.request())));
                waiting.addLast(submitted.task().id());
            }
            case Joined joined -> {
                requests.get(joined.taskId()).add(joined.request());
                owners.put(joined.request().id(), joined.taskId());
            }
            case Started started -> {
                if (started.attempt() == 1) {
                    waiting.removeFirst();
                }
                current = started.taskId();
                attempt = started.attempt();
                running = true;
                deadline = started.deadline();
            }
            case Ended ended -> {
                running = false;
                deadline = 0;
                resumeAt = ended.resumeAt();
                if (ended.finished()) {
                    tasks.remove(ended.taskId());
                    requests.remove(ended.taskId()).forEach(request -> owners.remove(request.id()));
                    current = null;
                    attempt = 0;
                } else {
                    attempt = ended.attempt() + 1;
                }
            }
        }
    }

    /**
     * The attempt a {@link Started} hands out, before or after it is applied.
     */
    public Attempt attempt(Started started) {
        return new Attempt(tasks.get(started.taskId()), started.attempt());
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
        final List<Work> queued = waiting.stream().map(this::work).toList();
        return new PartitionSnapshot(queued, current == null ? null : work(current), attempt, running, deadline, resumeAt);
    }

    /**
     * Nothing waiting, nothing in progress; the pause may still last.
     */
    public boolean isEmpty() {
        return !running && current == null && waiting.isEmpty();
    }

    /**
     * Empty and the pause is over: the engine may drop the partition - a task of the key coming later may start at once.
     */
    public boolean isIdle(long now) {
        return isEmpty() && now >= resumeAt;
    }

    private void add(Work work) {
        final String id = work.task().id();
        tasks.put(id, work.task());
        requests.put(id, new ArrayList<>(work.requests()));
        work.requests().forEach(request -> owners.put(request.id(), id));
    }

    private Work work(String taskId) {
        return new Work(tasks.get(taskId), requests.get(taskId));
    }

    private Started start(
        String taskId,
        int number,
        long now
    ) {
        return new Started(taskId, number, now + settings.attemptTimeout().toMillis());
    }

    /**
     * The attempt in progress ended with the result: the task finishes or retries, the pause starts; without a pause
     * the next attempt goes out at once.
     */
    private Step end(AttemptResult result, long now) {
        final Duration pause = key == null
            ? Duration.ZERO
            : delay.after(key, result);
        final long resume = pause.isPositive()
            ? now + pause.toMillis()
            : now;
        final TaskResult finished = switch (result.status()) {
            case DONE -> finish(TaskStatus.DONE, result.body(), null);
            case FAILED -> finish(TaskStatus.FAILED, null, result.error());
            case RETRY -> attempt >= settings.maxAttempts()
                ? finish(TaskStatus.FAILED, null, "attempts exhausted, last error: " + result.error())
                : null;
        };
        final List<PartitionEvent> events = new ArrayList<>(2);
        events.add(new Ended(current, attempt, finished != null, resume));
        if (resume <= now) {
            if (finished == null) {
                events.add(start(current, attempt + 1, now));
            } else if (!waiting.isEmpty()) {
                events.add(start(waiting.peekFirst(), 1, now));
            }
        }
        return new Step(events, finished == null ? null : new Step.Finished(finished, requests.get(current)));
    }

    private TaskResult finish(
        TaskStatus status,
        Blob body,
        String error
    ) {
        final Map<String, String> meta = new HashMap<>(tasks.get(current).meta());
        meta.put(TaskQueue.ATTEMPTS, String.valueOf(attempt));
        return new TaskResult(current, status, body, error, meta);
    }

    private AttemptResult timedOut() {
        return new AttemptResult(current, attempt, tasks.get(current).meta(), AttemptStatus.RETRY, null,
            "no result within " + settings.attemptTimeout());
    }
}
