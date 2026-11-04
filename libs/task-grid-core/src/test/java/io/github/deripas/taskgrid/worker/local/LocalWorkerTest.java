package io.github.deripas.taskgrid.worker.local;

import io.github.deripas.taskgrid.task.Attempt;
import io.github.deripas.taskgrid.task.AttemptResult;
import io.github.deripas.taskgrid.task.AttemptStatus;
import io.github.deripas.taskgrid.task.Blob;
import io.github.deripas.taskgrid.task.Task;
import io.github.deripas.taskgrid.worker.Assignment;
import io.github.deripas.taskgrid.worker.Worker;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.RejectedExecutionException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;

class LocalWorkerTest {
    private final List<AttemptResult> results = new ArrayList<>();
    private final Map<String, Handler> handlers = Map.of(
        "echo", attempt -> attempt.task().payload(),
        "invalid", attempt -> {
            throw new IllegalArgumentException("bad input");
        },
        "flaky", attempt -> {
            throw new RetryableException("connection reset");
        });
    private final LocalWorker worker = new LocalWorker(handlers, Runnable::run, results::add);

    @Test
    void returnedBodyIsDone() {
        final Attempt attempt = attempt("echo");
        assertInstanceOf(Assignment.Accepted.class, assign(attempt));
        assertEquals(List.of(AttemptResult.done(attempt, attempt.task().payload())), results);
    }

    @Test
    void retryableExceptionIsRetry() {
        assign(attempt("flaky"));
        assertEquals(AttemptStatus.RETRY, results.getFirst().status());
        assertEquals("connection reset", results.getFirst().error());
    }

    @Test
    void anyOtherExceptionIsFailed() {
        assign(attempt("invalid"));
        assertEquals(AttemptStatus.FAILED, results.getFirst().status());
        assertEquals("java.lang.IllegalArgumentException: bad input", results.getFirst().error());
    }

    @Test
    void rejectsWithoutKindOrHandler() {
        final Attempt noKind = new Attempt(new Task("t", Blob.of(new byte[0]), Map.of()), 1);
        assertEquals(new Assignment.Rejected("no kind in task meta"), assign(noKind));
        assertEquals(new Assignment.Rejected("no handler for kind render"), assign(attempt("render")));
        assertEquals(List.of(), results);
    }

    @Test
    void rejectsWhenTheExecutorRefuses() {
        final LocalWorker refusing = new LocalWorker(handlers, command -> {
            throw new RejectedExecutionException("shut down");
        }, results::add);
        assertEquals(new Assignment.Rejected("executor rejected the attempt: shut down"),
            refusing.assign(attempt("echo")).toCompletableFuture().join());
    }

    private Assignment assign(Attempt attempt) {
        return worker.assign(attempt).toCompletableFuture().join();
    }

    private static Attempt attempt(String kind) {
        return new Attempt(new Task("t", Blob.of(new byte[]{42}), Map.of(Worker.KIND, kind)), 1);
    }
}
