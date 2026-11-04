package io.github.deripas.taskgrid.result.memory;

import io.github.deripas.taskgrid.result.ResultStore;
import io.github.deripas.taskgrid.task.TaskResult;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;

/**
 * Result store of a single node, in memory. A result is kept for {@code ttl} after it is put; expired results are
 * evicted on the next call, without a background thread. Results are lost when the process stops.
 */
public final class MemoryResultStore implements ResultStore {
    private final Object lock = new Object();
    private final Duration ttl;
    private final Clock clock;
    /**
     * In put order, which is also expiry order.
     */
    private final LinkedHashMap<String, Stored> results = new LinkedHashMap<>();
    private final Map<String, List<CompletableFuture<Optional<TaskResult>>>> waiters = new HashMap<>();

    public MemoryResultStore(Duration ttl, Clock clock) {
        if (!ttl.isPositive()) {
            throw new IllegalArgumentException("ttl must be positive: " + ttl);
        }
        this.ttl = ttl;
        this.clock = clock;
    }

    @Override
    public CompletionStage<Void> put(TaskResult result) {
        final List<CompletableFuture<Optional<TaskResult>>> waiting;
        synchronized (lock) {
            final Instant now = clock.instant();
            evictExpired(now);
            results.remove(result.taskId());
            results.put(result.taskId(), new Stored(result, now.plus(ttl)));
            waiting = waiters.remove(result.taskId());
        }
        if (waiting != null) {
            final Optional<TaskResult> found = Optional.of(result);
            waiting.forEach(waiter -> waiter.complete(found));
        }
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public CompletionStage<Optional<TaskResult>> await(String taskId, Duration timeout) {
        final CompletableFuture<Optional<TaskResult>> waiter = new CompletableFuture<>();
        synchronized (lock) {
            evictExpired(clock.instant());
            final Stored stored = results.get(taskId);
            if (stored != null) {
                return CompletableFuture.completedFuture(Optional.of(stored.result()));
            }
            if (!timeout.isPositive()) {
                return CompletableFuture.completedFuture(Optional.empty());
            }
            waiters.computeIfAbsent(taskId, id -> new ArrayList<>()).add(waiter);
        }
        waiter.completeOnTimeout(Optional.empty(), timeout.toNanos(), TimeUnit.NANOSECONDS);
        waiter.whenComplete((result, error) -> forget(taskId, waiter));
        return waiter;
    }

    private void forget(String taskId, CompletableFuture<Optional<TaskResult>> waiter) {
        synchronized (lock) {
            final List<CompletableFuture<Optional<TaskResult>>> waiting = waiters.get(taskId);
            if (waiting != null && waiting.remove(waiter) && waiting.isEmpty()) {
                waiters.remove(taskId);
            }
        }
    }

    private void evictExpired(Instant now) {
        final Iterator<Stored> it = results.values().iterator();
        while (it.hasNext()) {
            if (it.next().expiresAt().isAfter(now)) {
                return;
            }
            it.remove();
        }
    }

    private record Stored(TaskResult result, Instant expiresAt) {
    }
}
