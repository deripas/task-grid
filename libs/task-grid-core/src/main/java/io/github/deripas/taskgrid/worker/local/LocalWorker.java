package io.github.deripas.taskgrid.worker.local;

import io.github.deripas.taskgrid.task.Attempt;
import io.github.deripas.taskgrid.task.AttemptResult;
import io.github.deripas.taskgrid.worker.Assignment;
import io.github.deripas.taskgrid.worker.AttemptCallback;
import io.github.deripas.taskgrid.worker.Worker;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;

/**
 * Worker executing attempts in this JVM: picks the handler by {@link Worker#KIND} and runs it on the executor.
 * Concurrency is bounded only by the executor.
 */
@Slf4j
public final class LocalWorker implements Worker {
    private final Map<String, Handler> handlers;
    private final Executor executor;
    private final List<AttemptCallback> callbacks = new CopyOnWriteArrayList<>();

    /**
     * @param handlers by kind
     */
    public LocalWorker(Map<String, Handler> handlers, Executor executor) {
        this.handlers = Map.copyOf(handlers);
        this.executor = executor;
    }

    @Override
    public CompletionStage<Assignment> assign(Attempt attempt) {
        final String kind = attempt.task().meta().get(KIND);
        if (kind == null) {
            return rejected("no " + KIND + " in task meta");
        }
        final Handler handler = handlers.get(kind);
        if (handler == null) {
            return rejected("no handler for " + KIND + " " + kind);
        }
        try {
            executor.execute(() -> report(execute(handler, attempt)));
        } catch (RejectedExecutionException e) {
            return rejected("executor rejected the attempt: " + e.getMessage());
        }
        return CompletableFuture.completedFuture(new Assignment.Accepted());
    }

    /**
     * Results are reported on the thread of the executor that ran the handler.
     */
    @Override
    public void onComplete(AttemptCallback callback) {
        callbacks.add(callback);
    }

    private void report(AttemptResult result) {
        for (AttemptCallback callback : callbacks) {
            try {
                callback.complete(result);
            } catch (RuntimeException e) {
                log.error("Attempt callback failed: task={} attempt={}", result.taskId(), result.attempt(), e);
            }
        }
    }

    private static AttemptResult execute(Handler handler, Attempt attempt) {
        try {
            return AttemptResult.done(attempt, handler.handle(attempt));
        } catch (RetryableException e) {
            return AttemptResult.retry(attempt, e.getMessage());
        } catch (Exception e) {
            log.debug("Attempt failed: task={} attempt={}", attempt.task().id(), attempt.number(), e);
            return AttemptResult.failed(attempt, String.valueOf(e));
        }
    }

    private static CompletionStage<Assignment> rejected(String reason) {
        return CompletableFuture.completedFuture(new Assignment.Rejected(reason));
    }
}
