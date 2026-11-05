package io.github.deripas.taskgrid.dispatch;

import io.github.deripas.taskgrid.queue.ReadyCallback;
import io.github.deripas.taskgrid.queue.ResultCallback;
import io.github.deripas.taskgrid.queue.TaskQueue;
import io.github.deripas.taskgrid.task.Attempt;
import io.github.deripas.taskgrid.task.AttemptResult;
import io.github.deripas.taskgrid.task.AttemptStatus;
import io.github.deripas.taskgrid.task.Blob;
import io.github.deripas.taskgrid.task.Task;
import io.github.deripas.taskgrid.worker.Assignment;
import io.github.deripas.taskgrid.worker.AttemptCallback;
import io.github.deripas.taskgrid.worker.Worker;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import static org.junit.jupiter.api.Assertions.assertEquals;

class DispatcherTest {
    private final FakeQueue queue = new FakeQueue();
    private final FakeWorker worker = new FakeWorker();

    @BeforeEach
    void connect() {
        Dispatcher.connect(queue, worker);
    }

    @Test
    void readyAttemptGoesToTheWorkerAndItsResultBackToTheQueue() {
        final Attempt attempt = attempt();
        worker.answer = CompletableFuture.completedFuture(new Assignment.Accepted());
        queue.ready.ready(attempt);
        final AttemptResult result = AttemptResult.done(attempt, Blob.of(new byte[]{1}));
        worker.callback.complete(result);

        assertEquals(List.of(attempt), worker.assigned);
        assertEquals(List.of(result), queue.completed);
    }

    @Test
    void rejectedAttemptFailsItsTask() {
        worker.answer = CompletableFuture.completedFuture(new Assignment.Rejected("no handler"));
        queue.ready.ready(attempt());

        assertEquals(AttemptStatus.FAILED, queue.completed.getFirst().status());
        assertEquals("rejected: no handler", queue.completed.getFirst().error());
    }

    @Test
    void failedAssignmentFailsItsTask() {
        worker.answer = CompletableFuture.failedFuture(new IllegalStateException("broken"));
        queue.ready.ready(attempt());

        assertEquals(AttemptStatus.FAILED, queue.completed.getFirst().status());
        assertEquals("assign failed: java.lang.IllegalStateException: broken", queue.completed.getFirst().error());
    }

    /**
     * The worker answers later: nothing goes to the queue until then.
     */
    @Test
    void waitsForTheAnswerOfTheWorker() {
        final CompletableFuture<Assignment> answer = new CompletableFuture<>();
        worker.answer = answer;
        queue.ready.ready(attempt());
        assertEquals(List.of(), queue.completed);

        answer.complete(new Assignment.Rejected("no credit"));
        assertEquals("rejected: no credit", queue.completed.getFirst().error());
    }

    private static Attempt attempt() {
        return new Attempt(new Task("t", Blob.of(new byte[0]), Map.of(Worker.KIND, "fetch")), 1);
    }

    private static final class FakeQueue implements TaskQueue {
        private final List<AttemptResult> completed = new ArrayList<>();
        private ReadyCallback ready;

        @Override
        public CompletionStage<Void> submit(Task task) {
            throw new UnsupportedOperationException();
        }

        @Override
        public CompletionStage<Void> complete(AttemptResult result) {
            completed.add(result);
            return CompletableFuture.completedFuture(null);
        }

        @Override
        public void onReady(ReadyCallback callback) {
            ready = callback;
        }

        @Override
        public void onResult(ResultCallback callback) {
            throw new UnsupportedOperationException();
        }
    }

    private static final class FakeWorker implements Worker {
        private final List<Attempt> assigned = new ArrayList<>();
        private CompletionStage<Assignment> answer;
        private AttemptCallback callback;

        @Override
        public CompletionStage<Assignment> assign(Attempt attempt) {
            assigned.add(attempt);
            return answer;
        }

        @Override
        public void onComplete(AttemptCallback callback) {
            this.callback = callback;
        }
    }
}
