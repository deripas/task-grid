package io.github.deripas.taskgrid.queue.pekko;

import io.github.deripas.taskgrid.queue.ReadyCallback;
import io.github.deripas.taskgrid.queue.ResultCallback;
import io.github.deripas.taskgrid.task.Attempt;
import io.github.deripas.taskgrid.task.Request;
import io.github.deripas.taskgrid.task.TaskResult;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * Receivers of this node: the partitions living here hand their attempts and final results to them.
 */
@Slf4j
final class Receivers {
    private final List<ReadyCallback> ready = new CopyOnWriteArrayList<>();
    private final List<ResultCallback> callbacks = new CopyOnWriteArrayList<>();

    void onReady(ReadyCallback callback) {
        ready.add(callback);
    }

    void onResult(ResultCallback callback) {
        callbacks.add(callback);
    }

    void ready(Attempt attempt) {
        for (ReadyCallback callback : ready) {
            try {
                callback.ready(attempt);
            } catch (RuntimeException e) {
                log.error("Ready callback failed: task={} attempt={}", attempt.task().id(), attempt.number(), e);
            }
        }
    }

    /**
     * To every callback; completes normally when all of them did.
     */
    CompletionStage<Void> deliver(TaskResult result, List<Request> requests) {
        final List<CompletableFuture<Void>> stages = new ArrayList<>(callbacks.size());
        for (ResultCallback callback : callbacks) {
            stages.add(call(() -> callback.finished(result, requests)));
        }
        return CompletableFuture.allOf(stages.toArray(CompletableFuture[]::new));
    }

    private static CompletableFuture<Void> call(Delivery delivery) {
        try {
            return delivery.start().toCompletableFuture();
        } catch (RuntimeException e) {
            return CompletableFuture.failedFuture(e);
        }
    }

    /**
     * One receiver of a result.
     */
    private interface Delivery {
        CompletionStage<Void> start();
    }
}
