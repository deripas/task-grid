package io.github.deripas.taskgrid.worker.remote;

import io.github.deripas.taskgrid.task.Attempt;
import io.github.deripas.taskgrid.task.AttemptResult;
import io.github.deripas.taskgrid.task.Blob;
import io.github.deripas.taskgrid.worker.Assignment;
import io.github.deripas.taskgrid.worker.AttemptCallback;
import io.github.deripas.taskgrid.worker.Worker;
import lombok.extern.slf4j.Slf4j;

import java.time.Clock;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Worker for handlers behind handler-sidecar: attempts go to the sidecars connected to this node, by their credit.
 * <p>
 * Engine of the {@link Allocator}s (one per kind) for a single node: all events run one by one on a single thread,
 * plus a periodic tick. Results of attempts and retries of the attempts of closed sessions are reported to the
 * callback from that thread. A transport adapter connects sidecars through {@link Sidecars}.
 */
@Slf4j
public final class RemoteWorker implements Worker, Sidecars, AutoCloseable {
    private final RemoteSettings settings;
    private final AttemptCallback callback;
    private final Clock clock;
    private final ScheduledExecutorService engine;
    // engine thread only
    private final Map<String, Allocator> allocators = new HashMap<>();

    public RemoteWorker(
        RemoteSettings settings,
        AttemptCallback callback,
        Clock clock
    ) {
        this.settings = settings;
        this.callback = callback;
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
            reply.complete(new Assignment.Rejected("node stopping"));
        }
        return reply;
    }

    @Override
    public Session connect(
        String workerId,
        String kind,
        int capacity,
        SessionChannel channel
    ) {
        final String sessionId = UUID.randomUUID().toString();
        if (!post(() -> allocator(kind).join(sessionId, workerId, capacity, channel, now()))) {
            channel.close();
        }
        return new Session() {
            @Override
            public void credit(long limit, long version) {
                post(() -> allocator(kind).credited(sessionId, limit, version, now()));
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

    @Override
    public void complete(String workerId, AttemptResult result) {
        final boolean posted = post(() -> {
            allocators.values().forEach(a -> a.completed(workerId, result.taskId(), result.attempt()));
            callback.complete(result);
        });
        if (!posted) {
            log.debug("Ignoring a result after stop: worker={} task={}", workerId, result.taskId());
        }
    }

    /**
     * Closes the sessions, rejects waiting attempts and stops the engine thread.
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
    }

    private Allocator allocator(String kind) {
        return allocators.computeIfAbsent(kind, k -> new Allocator(k, settings, callback));
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
