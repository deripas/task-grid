package io.github.deripas.taskgrid.worker.delayed;

import io.github.deripas.taskgrid.queue.TaskQueue;
import io.github.deripas.taskgrid.task.Attempt;
import io.github.deripas.taskgrid.task.AttemptResult;
import io.github.deripas.taskgrid.worker.Assignment;
import io.github.deripas.taskgrid.worker.AttemptCallback;
import io.github.deripas.taskgrid.worker.Worker;
import lombok.extern.slf4j.Slf4j;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

/**
 * Worker decorator: the next attempt of a partition key ({@link TaskQueue#PARTITION_KEY}) goes to the wrapped worker not earlier than
 * {@link DelayPolicy#after} the result of the previous one (finish to start: the pause counts from the moment
 * the result came, however long the attempt took).
 * <ul>
 *   <li>The queue runs one attempt of a partition at a time, so there is one pause per key; this worker only holds
 *   the next attempt back. It keeps nothing but the end of the pause of keys that have one.</li>
 *   <li>Every result counts - DONE, FAILED, RETRY: each was a request to the site. Retries wait too.</li>
 *   <li>An attempt the wrapped worker rejected made no request: no pause after it.</li>
 *   <li>Tasks without a partition key and the first attempt of a key are not held back.</li>
 * </ul>
 * The pause ends when it is due, not when it is polled: a timer of its own thread hands the attempt over.
 */
@Slf4j
public final class DelayedWorker implements Worker, AutoCloseable {
    private final Object lock = new Object();
    private final DelayPolicy policy;
    private final AttemptCallback callback;
    private final Worker worker;
    private final ScheduledExecutorService timer;
    /**
     * Key -> end of its pause, System.nanoTime.
     */
    private final Map<String, Long> pausedUntil = new HashMap<>();
    /**
     * Attempts handed to the wrapped worker -> their keys: a result tells only the task id and the attempt number.
     */
    private final Map<InFlight, String> inFlight = new HashMap<>();

    /**
     * @param callback the queue: results go there, after the pause of their key is set
     * @param worker   creates the wrapped worker: this one is the callback it reports results to
     */
    public DelayedWorker(
        DelayPolicy policy,
        AttemptCallback callback,
        Function<AttemptCallback, Worker> worker
    ) {
        this.policy = policy;
        this.callback = callback;
        this.timer = Executors.newSingleThreadScheduledExecutor(r -> {
            final Thread thread = new Thread(r, "delayed-worker");
            thread.setDaemon(true);
            return thread;
        });
        this.worker = worker.apply(this::completed);
    }

    @Override
    public CompletionStage<Assignment> assign(Attempt attempt) {
        final String key = attempt.task().meta().get(TaskQueue.PARTITION_KEY);
        if (key == null) {
            return worker.assign(attempt);
        }
        final long wait;
        synchronized (lock) {
            wait = remaining(key);
            inFlight.put(InFlight.of(attempt), key);
        }
        if (wait <= 0) {
            return handOver(attempt);
        }
        log.debug("Attempt held back: task={} attempt={} key={} ms={}",
            attempt.task().id(), attempt.number(), key, TimeUnit.NANOSECONDS.toMillis(wait));
        final CompletableFuture<Assignment> assignment = new CompletableFuture<>();
        try {
            timer.schedule(() -> handOver(attempt).whenComplete((a, error) -> {
                if (error == null) {
                    assignment.complete(a);
                } else {
                    assignment.completeExceptionally(error);
                }
            }), wait, TimeUnit.NANOSECONDS);
        } catch (RejectedExecutionException e) {
            forget(attempt);
            return CompletableFuture.completedFuture(new Assignment.Rejected("delayed worker is closed"));
        }
        return assignment;
    }

    /**
     * Stops the timer: attempts still held back are not handed over. The wrapped worker is closed by its owner.
     */
    @Override
    public void close() {
        timer.shutdownNow();
    }

    /**
     * A result of the wrapped worker: the pause of the key starts, then the queue learns the result - it may assign
     * the next attempt of the key at once, and that one must see the pause.
     */
    private void completed(AttemptResult result) {
        synchronized (lock) {
            final String key = inFlight.remove(new InFlight(result.taskId(), result.attempt()));
            if (key != null) {
                pause(key, policy.after(key, result));
            }
        }
        callback.complete(result);
    }

    private CompletionStage<Assignment> handOver(Attempt attempt) {
        final CompletionStage<Assignment> assignment;
        try {
            assignment = worker.assign(attempt);
        } catch (RuntimeException e) {
            forget(attempt);
            return CompletableFuture.failedFuture(e);
        }
        return assignment.whenComplete((a, error) -> {
            if (error != null || a instanceof Assignment.Rejected) {
                forget(attempt);
            }
        });
    }

    /**
     * Under the lock.
     */
    private void pause(String key, Duration delay) {
        if (!delay.isPositive()) {
            return;
        }
        final long until = System.nanoTime() + delay.toNanos();
        pausedUntil.put(key, until);
        try {
            // the key is forgotten when its pause is over, also if no attempt of it comes any more
            timer.schedule(() -> expire(key, until), delay.toNanos(), TimeUnit.NANOSECONDS);
        } catch (RejectedExecutionException e) {
            log.debug("Delayed worker is closed, pause of key {} is not tracked", key);
        }
    }

    /**
     * Nanoseconds left of the pause of the key; under the lock.
     */
    private long remaining(String key) {
        final Long until = pausedUntil.get(key);
        return until == null
            ? 0
            : until - System.nanoTime();
    }

    private void expire(String key, long until) {
        synchronized (lock) {
            pausedUntil.remove(key, until);
        }
    }

    private void forget(Attempt attempt) {
        synchronized (lock) {
            inFlight.remove(InFlight.of(attempt));
        }
    }

    /**
     * An attempt handed to the wrapped worker, as its result names it.
     */
    private record InFlight(String taskId, int attempt) {
        static InFlight of(Attempt attempt) {
            return new InFlight(attempt.task().id(), attempt.number());
        }
    }
}
