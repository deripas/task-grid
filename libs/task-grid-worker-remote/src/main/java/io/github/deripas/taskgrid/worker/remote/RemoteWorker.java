package io.github.deripas.taskgrid.worker.remote;

import io.github.deripas.taskgrid.task.Attempt;
import io.github.deripas.taskgrid.task.AttemptResult;
import io.github.deripas.taskgrid.task.Blob;
import io.github.deripas.taskgrid.worker.Assignment;
import io.github.deripas.taskgrid.worker.AttemptCallback;
import io.github.deripas.taskgrid.worker.Worker;
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
import java.util.concurrent.TimeUnit;

/**
 * Worker for handlers behind handler-sidecar: attempts go to the sidecars connected to this node, by their credit.
 * <p>
 * Engine of the {@link Allocator}s (one per kind) for a single node: all events run one by one on a single thread,
 * plus a periodic tick. Results of attempts and retries of the attempts of closed sessions are reported to the
 * callbacks from that thread. A transport adapter connects sidecars through {@link Sidecars}.
 * <p>
 * {@link #load} by kind is refreshed on each tick: the metrics of the pools.
 */
@Slf4j
public final class RemoteWorker implements Worker, Sidecars, AutoCloseable {
    private final RemoteSettings settings;
    private final Clock clock;
    private final ScheduledExecutorService engine;
    private final List<AttemptCallback> callbacks = new CopyOnWriteArrayList<>();
    // engine thread only
    private final Map<String, Allocator> allocators = new HashMap<>();
    private volatile Map<String, Load> load = Map.of();

    public RemoteWorker(RemoteSettings settings, Clock clock) {
        this.settings = settings;
        this.clock = clock;
        this.engine = Executors.newSingleThreadScheduledExecutor(Thread.ofPlatform().name("remote-worker").daemon().factory());
        final long tick = settings.tick().toNanos();
        engine.scheduleWithFixedDelay(() -> safely(this::tick), tick, tick, TimeUnit.NANOSECONDS);
    }

    @Override
    public CompletionStage<Assignment> assign(Attempt attempt) {
        final String kind = attempt.task().meta().get(KIND);
        if (kind == null) {
            return CompletableFuture.completedFuture(new Assignment.Rejected("no kind in task meta"));
        }
        if (!(attempt.task().payload() instanceof Blob.Inline)) {
            return CompletableFuture.completedFuture(new Assignment.Rejected("only inline payloads are sent to sidecars"));
        }
        final CompletableFuture<Assignment> reply = new CompletableFuture<>();
        if (!post(() -> allocator(kind).assign(attempt, reply, now()))) {
            // stopped: not a failure of the task - another node of a cluster runs it again
            reply.complete(new Assignment.Accepted());
            report(AttemptResult.retry(attempt, "node stopping"));
        }
        return reply;
    }

    /**
     * Results are reported on the engine thread; after stop - on the thread of the call.
     */
    @Override
    public void onComplete(AttemptCallback callback) {
        callbacks.add(callback);
    }

    /**
     * Load by kind as of the last tick: kinds that ever had sessions or attempts on this node.
     */
    public Map<String, Load> load() {
        return load;
    }

    @Override
    public Session connect(SidecarHello hello, SessionChannel channel) {
        final String sessionId = UUID.randomUUID().toString();
        final String kind = hello.kind();
        if (!post(() -> allocator(kind).join(sessionId, hello, channel, now()))) {
            channel.close();
        }
        return new Session() {
            @Override
            public void credit(long limit, long version) {
                post(() -> allocator(kind).credited(sessionId, limit, version, now()));
            }

            @Override
            public void share(double capacity) {
                post(() -> allocator(kind).shared(sessionId, capacity));
            }

            @Override
            public void drain() {
                post(() -> allocator(kind).drain(sessionId, now()));
            }

            @Override
            public void closed(String reason) {
                post(() -> allocator(kind).leave(sessionId, reason, now()));
            }
        };
    }

    /**
     * Completes when every callback has taken the result; after stop - fails, the sidecar sends it to another node.
     */
    @Override
    public CompletionStage<Void> complete(String workerId, AttemptResult result) {
        final CompletableFuture<Void> taken = new CompletableFuture<>();
        final boolean posted = post(() -> {
            allocators.values().forEach(a -> a.completed(workerId, result.taskId(), result.attempt()));
            report(result).whenComplete((ignored, error) -> {
                if (error == null) {
                    taken.complete(null);
                } else {
                    taken.completeExceptionally(error);
                }
            });
        });
        if (!posted) {
            log.debug("Result after stop, not taken: worker={} task={}", workerId, result.taskId());
            taken.completeExceptionally(new IllegalStateException("node stopping"));
        }
        return taken;
    }

    /**
     * Closes the sessions, reports waiting attempts as RETRY and stops the engine thread.
     */
    @Override
    public void close() {
        post(() -> allocators.values().forEach(Allocator::stop));
        engine.shutdown();
        try {
            if (!engine.awaitTermination(5, TimeUnit.SECONDS)) {
                log.warn("Engine did not stop in time");
                engine.shutdownNow();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            engine.shutdownNow();
        }
    }

    private void tick() {
        final long now = now();
        allocators.values().forEach(a -> a.tick(now));
        final Map<String, Load> snapshot = new HashMap<>();
        allocators.forEach((kind, allocator) -> snapshot.put(kind, allocator.load()));
        load = Map.copyOf(snapshot);
    }

    private Allocator allocator(String kind) {
        return allocators.computeIfAbsent(kind, k -> new Allocator(k, settings, this::report));
    }

    /**
     * Engine thread (after stop - any). Completes when all callbacks have taken the result.
     */
    private CompletableFuture<Void> report(AttemptResult result) {
        final List<CompletableFuture<Void>> stages = new ArrayList<>(callbacks.size());
        for (AttemptCallback callback : callbacks) {
            try {
                stages.add(callback.complete(result).toCompletableFuture());
            } catch (RuntimeException e) {
                log.error("Attempt callback failed: task={} attempt={}", result.taskId(), result.attempt(), e);
                stages.add(CompletableFuture.failedFuture(e));
            }
        }
        return CompletableFuture.allOf(stages.toArray(CompletableFuture[]::new));
    }

    /**
     * {@code false} - the engine is stopped, the event is dropped.
     */
    private boolean post(Runnable event) {
        try {
            engine.execute(() -> safely(event));
            return true;
        } catch (RejectedExecutionException e) {
            return false;
        }
    }

    private long now() {
        return clock.millis();
    }

    /**
     * An exception must not stop the engine thread (and with it the periodic tick).
     */
    private static void safely(Runnable event) {
        try {
            event.run();
        } catch (RuntimeException e) {
            log.error("Event failed", e);
        }
    }
}
