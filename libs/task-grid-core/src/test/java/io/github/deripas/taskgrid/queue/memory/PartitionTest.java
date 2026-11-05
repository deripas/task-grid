package io.github.deripas.taskgrid.queue.memory;

import io.github.deripas.taskgrid.queue.DelayPolicy;
import io.github.deripas.taskgrid.queue.QueueSettings;
import io.github.deripas.taskgrid.queue.TaskQueue;
import io.github.deripas.taskgrid.task.Attempt;
import io.github.deripas.taskgrid.task.AttemptResult;
import io.github.deripas.taskgrid.task.Blob;
import io.github.deripas.taskgrid.task.Task;
import io.github.deripas.taskgrid.task.TaskResult;
import io.github.deripas.taskgrid.task.TaskStatus;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class PartitionTest {
    private static final Duration PAUSE = Duration.ofSeconds(1);

    private final Recorder out = new Recorder();
    private final Partition partition = new Partition("a.com", new QueueSettings(3), DelayPolicy.NONE, out);

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
    void nextTaskWaitsForThePauseAfterTheResult() {
        final Partition paused = new Partition("a.com", new QueueSettings(3), DelayPolicy.fixed(PAUSE), out);
        paused.submit(task("a"));
        paused.submit(task("b"));
        paused.completed(AttemptResult.done(out.attempts.getFirst(), Blob.of(new byte[0])));

        assertEquals(List.of("a#1"), out.assigned());
        assertEquals(List.of(PAUSE), out.pauses);
        assertEquals("a", out.results.getFirst().taskId());
        assertFalse(paused.isIdle());

        paused.resume();
        assertEquals(List.of("a#1", "b#1"), out.assigned());
    }

    @Test
    void retryWaitsForThePauseToo() {
        final Partition paused = new Partition("a.com", new QueueSettings(3), DelayPolicy.fixed(PAUSE), out);
        paused.submit(task("a"));
        paused.completed(AttemptResult.retry(out.attempts.getFirst(), "busy"));
        assertEquals(List.of("a#1"), out.assigned());

        paused.resume();
        assertEquals(List.of("a#1", "a#2"), out.assigned());
    }

    /**
     * The pause holds the partition even when it has nothing to do: a task of the key submitted during the pause waits.
     */
    @Test
    void pauseOutlivesTheLastTask() {
        final Partition paused = new Partition("a.com", new QueueSettings(3), DelayPolicy.fixed(PAUSE), out);
        paused.submit(task("a"));
        paused.completed(AttemptResult.failed(out.attempts.getFirst(), "bad url"));
        assertFalse(paused.isIdle());

        paused.submit(task("b"));
        assertEquals(List.of("a#1"), out.assigned());

        paused.resume();
        assertEquals(List.of("a#1", "b#1"), out.assigned());
        paused.completed(AttemptResult.done(out.attempts.get(1), Blob.of(new byte[0])));
        paused.resume();
        assertTrue(paused.isIdle());
    }

    @Test
    void independentTaskHasNoPause() {
        final Partition independent = new Partition(null, new QueueSettings(3), DelayPolicy.fixed(PAUSE), out);
        independent.submit(task("a"));
        independent.completed(AttemptResult.done(out.attempts.getFirst(), Blob.of(new byte[0])));

        assertEquals(List.of(), out.pauses);
        assertTrue(independent.isIdle());
    }

    /**
     * A task submitted by a result callback does not start a second attempt next to the one the partition starts.
     */
    @Test
    void taskSubmittedFromTheResultRunsInOrder() {
        final Recorder reentrant = new Recorder();
        final Partition[] self = new Partition[1];
        reentrant.onFinished = result -> self[0].submit(task("c"));
        self[0] = new Partition("a.com", new QueueSettings(3), DelayPolicy.NONE, reentrant);
        self[0].submit(task("a"));
        self[0].submit(task("b"));
        self[0].completed(AttemptResult.done(reentrant.attempts.getFirst(), Blob.of(new byte[0])));

        assertEquals(List.of("a#1", "b#1"), reentrant.assigned());
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
        private final List<Duration> pauses = new ArrayList<>();
        private Consumer<TaskResult> onFinished = result -> {
        };

        @Override
        public void assign(Attempt attempt) {
            attempts.add(attempt);
        }

        @Override
        public void finished(TaskResult result) {
            results.add(result);
            onFinished.accept(result);
        }

        @Override
        public void resumeAfter(Duration delay) {
            pauses.add(delay);
        }

        private List<String> assigned() {
            return attempts.stream().map(a -> a.task().id() + "#" + a.number()).toList();
        }
    }
}
