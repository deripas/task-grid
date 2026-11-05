package io.github.deripas.taskgrid.queue.partition;

import io.github.deripas.taskgrid.queue.DelayPolicy;
import io.github.deripas.taskgrid.queue.QueueSettings;
import io.github.deripas.taskgrid.queue.TaskQueue;
import io.github.deripas.taskgrid.queue.partition.PartitionEvent.Ended;
import io.github.deripas.taskgrid.queue.partition.PartitionEvent.Started;
import io.github.deripas.taskgrid.task.Attempt;
import io.github.deripas.taskgrid.task.AttemptResult;
import io.github.deripas.taskgrid.task.Blob;
import io.github.deripas.taskgrid.task.Task;
import io.github.deripas.taskgrid.task.TaskResult;
import io.github.deripas.taskgrid.task.TaskStatus;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The state machine with time as a parameter: commands decide, {@link Partition#apply} changes the state.
 */
class PartitionTest {
    private static final long T0 = 1_000_000;
    private static final Duration PAUSE = Duration.ofSeconds(1);
    private static final Duration TIMEOUT = Duration.ofMinutes(1);
    private static final QueueSettings SETTINGS = new QueueSettings(3, TIMEOUT);

    private final List<PartitionEvent> journal = new ArrayList<>();
    private final Partition partition = new Partition("a.com", SETTINGS, DelayPolicy.NONE);

    @Test
    void runsTasksOneAtATimeInOrder() {
        run(partition, partition.submit(task("a"), T0));
        run(partition, partition.submit(task("b"), T0));
        assertEquals(List.of("a#1"), started());

        run(partition, partition.completed(done("a", 1), T0));
        assertEquals(List.of("a#1", "b#1"), started());
        assertEquals(TaskStatus.DONE, finished().getFirst().status());

        run(partition, partition.completed(done("b", 1), T0));
        assertFalse(partition.isIdle(T0), "results are not delivered yet");
        run(partition, partition.delivered("a"));
        run(partition, partition.delivered("b"));
        assertTrue(partition.isIdle(T0));
    }

    @Test
    void commandsDoNotChangeTheState() {
        partition.submit(task("a"), T0);
        assertEquals(List.of(new PartitionEvent.Submitted(task("a")), new Started(new Attempt(task("a"), 1), T0 + 60_000)),
            partition.submit(task("a"), T0));
    }

    @Test
    void retriesUntilAttemptsAreExhausted() {
        run(partition, partition.submit(task("a"), T0));
        run(partition, partition.completed(retry("a", 1, "io"), T0));
        run(partition, partition.completed(retry("a", 2, "io"), T0));
        run(partition, partition.completed(retry("a", 3, "timeout"), T0));

        assertEquals(List.of("a#1", "a#2", "a#3"), started());
        final TaskResult result = finished().getFirst();
        assertEquals(TaskStatus.FAILED, result.status());
        assertEquals("attempts exhausted, last error: timeout", result.error());
        assertEquals("3", result.meta().get(TaskQueue.ATTEMPTS));
    }

    @Test
    void failedAttemptFailsTheTaskAtOnce() {
        run(partition, partition.submit(task("a"), T0));
        run(partition, partition.completed(AttemptResult.failed(new Attempt(task("a"), 1), "bad url"), T0));

        assertEquals(List.of("a#1"), started());
        assertEquals("bad url", finished().getFirst().error());
    }

    @Test
    void sameIdIsNotQueuedTwiceAndAnotherPayloadIsAnError() {
        run(partition, partition.submit(task("a"), T0));
        assertEquals(List.of(), partition.submit(task("a"), T0));
        assertThrows(IllegalArgumentException.class,
            () -> partition.submit(new Task("a", Blob.of(new byte[]{9}), Map.of()), T0));
    }

    @Test
    void nextTaskWaitsForThePauseAfterTheResult() {
        final Partition paused = new Partition("a.com", SETTINGS, DelayPolicy.fixed(PAUSE));
        run(paused, paused.submit(task("a"), T0));
        run(paused, paused.submit(task("b"), T0));
        run(paused, paused.completed(done("a", 1), T0 + 100));

        assertEquals(List.of("a#1"), started());
        assertEquals(T0 + 100 + PAUSE.toMillis(), paused.nextDeadline());
        assertEquals(List.of(), paused.tick(T0 + 100 + PAUSE.toMillis() - 1));

        run(paused, paused.tick(T0 + 100 + PAUSE.toMillis()));
        assertEquals(List.of("a#1", "b#1"), started());
    }

    @Test
    void retryWaitsForThePauseToo() {
        final Partition paused = new Partition("a.com", SETTINGS, DelayPolicy.fixed(PAUSE));
        run(paused, paused.submit(task("a"), T0));
        run(paused, paused.completed(retry("a", 1, "busy"), T0));
        assertEquals(List.of("a#1"), started());

        run(paused, paused.tick(T0 + PAUSE.toMillis()));
        assertEquals(List.of("a#1", "a#2"), started());
    }

    /**
     * The pause holds the partition even when it has nothing to do: a task of the key submitted during the pause waits.
     */
    @Test
    void pauseOutlivesTheLastTask() {
        final Partition paused = new Partition("a.com", SETTINGS, DelayPolicy.fixed(PAUSE));
        run(paused, paused.submit(task("a"), T0));
        run(paused, paused.completed(done("a", 1), T0));
        run(paused, paused.delivered("a"));
        assertTrue(paused.isEmpty());
        assertFalse(paused.isIdle(T0), "empty, but the pause lasts");

        run(paused, paused.submit(task("b"), T0 + 10));
        assertEquals(List.of("a#1"), started());
        run(paused, paused.tick(T0 + PAUSE.toMillis()));
        assertEquals(List.of("a#1", "b#1"), started());
    }

    @Test
    void independentTaskHasNoPause() {
        final Partition independent = new Partition(null, SETTINGS, DelayPolicy.fixed(PAUSE));
        run(independent, independent.submit(task("a"), T0));
        run(independent, independent.completed(done("a", 1), T0));
        run(independent, independent.delivered("a"));

        assertTrue(independent.isIdle(T0));
    }

    /**
     * No result by the deadline (the worker is gone): the attempt counts as RETRY; its late result is ignored.
     */
    @Test
    void attemptWithoutAResultInTimeIsRetried() {
        run(partition, partition.submit(task("a"), T0));
        assertEquals(T0 + TIMEOUT.toMillis(), partition.nextDeadline());
        assertEquals(List.of(), partition.tick(T0 + TIMEOUT.toMillis() - 1));

        run(partition, partition.tick(T0 + TIMEOUT.toMillis()));
        assertEquals(List.of("a#1", "a#2"), started());
        assertEquals(List.of(), partition.completed(done("a", 1), T0 + TIMEOUT.toMillis() + 1));

        run(partition, partition.completed(done("a", 2), T0 + TIMEOUT.toMillis() + 2));
        assertEquals("2", finished().getFirst().meta().get(TaskQueue.ATTEMPTS));
    }

    @Test
    void ignoresResultsOfAttemptsNotInProgress() {
        run(partition, partition.submit(task("a"), T0));
        run(partition, partition.completed(retry("a", 1, "io"), T0));

        assertEquals(List.of(), partition.completed(done("a", 1), T0));
        assertEquals(List.of(), partition.completed(done("other", 2), T0));
    }

    /**
     * The journal replayed into a new partition gives the same state, whatever the clock and the settings are then.
     */
    @Test
    void replayRestoresTheState() {
        final Partition paused = new Partition("a.com", SETTINGS, DelayPolicy.fixed(PAUSE));
        run(paused, paused.submit(task("a"), T0));
        run(paused, paused.submit(task("b"), T0));
        run(paused, paused.completed(done("a", 1), T0));

        final Partition restored = new Partition("a.com", new QueueSettings(1, Duration.ofSeconds(1)), DelayPolicy.NONE);
        journal.forEach(restored::apply);

        assertEquals(paused.nextDeadline(), restored.nextDeadline());
        assertEquals(paused.undelivered(), restored.undelivered());
        assertEquals(List.of(new Started(new Attempt(task("b"), 1), T0 + PAUSE.toMillis() + 60_000)),
            paused.tick(T0 + PAUSE.toMillis()));
        assertEquals(paused.tick(T0 + PAUSE.toMillis()).size(), restored.tick(T0 + PAUSE.toMillis()).size());
    }

    @Test
    void snapshotRestoresTheState() {
        final Partition paused = new Partition("a.com", SETTINGS, DelayPolicy.fixed(PAUSE));
        run(paused, paused.submit(task("a"), T0));
        run(paused, paused.submit(task("b"), T0));
        run(paused, paused.submit(task("c"), T0));
        run(paused, paused.completed(done("a", 1), T0));

        final Partition restored = Partition.restore("a.com", SETTINGS, DelayPolicy.fixed(PAUSE), paused.snapshot());

        assertEquals(paused.snapshot(), restored.snapshot());
        assertEquals(List.of(), restored.submit(task("b"), T0), "known ids survive the snapshot");
        assertEquals(paused.tick(T0 + PAUSE.toMillis()), restored.tick(T0 + PAUSE.toMillis()));
    }

    private void run(Partition target, List<PartitionEvent> events) {
        events.forEach(target::apply);
        journal.addAll(events);
    }

    private List<String> started() {
        return journal.stream()
            .filter(e -> e instanceof Started)
            .map(e -> ((Started) e).attempt())
            .map(a -> a.task().id() + "#" + a.number())
            .toList();
    }

    private List<TaskResult> finished() {
        return journal.stream()
            .filter(e -> e instanceof Ended ended && ended.result() != null)
            .map(e -> ((Ended) e).result())
            .toList();
    }

    private static AttemptResult done(String id, int number) {
        return AttemptResult.done(new Attempt(task(id), number), Blob.of(new byte[0]));
    }

    private static AttemptResult retry(String id, int number, String error) {
        return AttemptResult.retry(new Attempt(task(id), number), error);
    }

    private static Task task(String id) {
        return new Task(id, Blob.of(id.getBytes(StandardCharsets.UTF_8)), Map.of());
    }
}
