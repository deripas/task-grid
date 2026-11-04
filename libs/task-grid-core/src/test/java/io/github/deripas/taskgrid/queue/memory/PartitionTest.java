package io.github.deripas.taskgrid.queue.memory;

import io.github.deripas.taskgrid.queue.QueueSettings;
import io.github.deripas.taskgrid.queue.TaskQueue;
import io.github.deripas.taskgrid.task.Attempt;
import io.github.deripas.taskgrid.task.AttemptResult;
import io.github.deripas.taskgrid.task.Blob;
import io.github.deripas.taskgrid.task.Task;
import io.github.deripas.taskgrid.task.TaskResult;
import io.github.deripas.taskgrid.task.TaskStatus;
import io.github.deripas.taskgrid.worker.Assignment;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class PartitionTest {
    private final Recorder out = new Recorder();
    private final Partition partition = new Partition(new QueueSettings(3), out);

    @Test
    void runsTasksOneAtATimeInOrder() {
        partition.submit(task("a"));
        partition.submit(task("b"));
        assertEquals(List.of("a#1"), out.assigned());

        partition.completed(AttemptResult.done(out.attempts.getFirst(), Blob.of(new byte[]{1})));
        assertEquals(List.of("a#1", "b#1"), out.assigned());
        assertEquals("a", out.results.getFirst().taskId());
        assertEquals(TaskStatus.DONE, out.results.getFirst().status());

        partition.completed(AttemptResult.done(out.attempts.get(1), Blob.of(new byte[0])));
        assertTrue(partition.isIdle());
    }

    @Test
    void retriesUntilAttemptsAreExhausted() {
        partition.submit(task("a"));
        partition.completed(AttemptResult.retry(out.attempts.get(0), "io"));
        partition.completed(AttemptResult.retry(out.attempts.get(1), "io"));
        assertFalse(partition.isIdle());
        partition.completed(AttemptResult.retry(out.attempts.get(2), "timeout"));

        assertEquals(List.of("a#1", "a#2", "a#3"), out.assigned());
        final TaskResult result = out.results.getFirst();
        assertEquals(TaskStatus.FAILED, result.status());
        assertEquals("attempts exhausted, last error: timeout", result.error());
        assertEquals("3", result.meta().get(TaskQueue.ATTEMPTS));
        assertTrue(partition.isIdle());
    }

    @Test
    void failedAttemptFailsTheTaskAtOnce() {
        partition.submit(task("a"));
        partition.completed(AttemptResult.failed(out.attempts.getFirst(), "bad url"));

        assertEquals(List.of("a#1"), out.assigned());
        assertEquals(TaskStatus.FAILED, out.results.getFirst().status());
        assertEquals("bad url", out.results.getFirst().error());
    }

    @Test
    void rejectedAssignmentFailsTheTaskAndStartsTheNext() {
        partition.submit(task("a"));
        partition.submit(task("b"));
        partition.assigned(out.attempts.getFirst(), new Assignment.Rejected("no handler"));

        assertEquals(TaskStatus.FAILED, out.results.getFirst().status());
        assertEquals("rejected: no handler", out.results.getFirst().error());
        assertEquals(List.of("a#1", "b#1"), out.assigned());
    }

    @Test
    void resultCarriesTaskMetaAndAttempts() {
        partition.submit(new Task("a", Blob.of(new byte[0]), Map.of("client.reply-to", "q1")));
        partition.completed(AttemptResult.done(out.attempts.getFirst(), Blob.of(new byte[0])));

        assertEquals(Map.of("client.reply-to", "q1", TaskQueue.ATTEMPTS, "1"), out.results.getFirst().meta());
    }

    @Test
    void ignoresResultsOfAttemptsNotInProgress() {
        partition.submit(task("a"));
        final Attempt first = out.attempts.getFirst();
        partition.completed(AttemptResult.retry(first, "io"));

        partition.completed(AttemptResult.done(first, Blob.of(new byte[0])));
        partition.completed(AttemptResult.done(new Attempt(task("other"), 2), Blob.of(new byte[0])));
        assertTrue(out.results.isEmpty());

        partition.completed(AttemptResult.done(out.attempts.get(1), Blob.of(new byte[0])));
        assertEquals("2", out.results.getFirst().meta().get(TaskQueue.ATTEMPTS));
    }

    private static Task task(String id) {
        return new Task(id, Blob.of(new byte[0]), Map.of());
    }

    private static final class Recorder implements Partition.Output {
        private final List<Attempt> attempts = new ArrayList<>();
        private final List<TaskResult> results = new ArrayList<>();

        @Override
        public void assign(Attempt attempt) {
            attempts.add(attempt);
        }

        @Override
        public void finished(TaskResult result) {
            results.add(result);
        }

        private List<String> assigned() {
            return attempts.stream().map(a -> a.task().id() + "#" + a.number()).toList();
        }
    }
}
