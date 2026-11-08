package io.github.deripas.taskgrid.queue.partition;

import io.github.deripas.taskgrid.queue.DelayPolicy;
import io.github.deripas.taskgrid.queue.QueueSettings;
import io.github.deripas.taskgrid.queue.TaskQueue;
import io.github.deripas.taskgrid.queue.partition.PartitionEvent.Joined;
import io.github.deripas.taskgrid.queue.partition.PartitionEvent.Started;
import io.github.deripas.taskgrid.queue.partition.PartitionEvent.Submitted;
import io.github.deripas.taskgrid.task.Attempt;
import io.github.deripas.taskgrid.task.AttemptResult;
import io.github.deripas.taskgrid.task.Blob;
import io.github.deripas.taskgrid.task.Request;
import io.github.deripas.taskgrid.task.RequestId;
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
import static org.junit.jupiter.api.Assertions.assertNull;
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
    private final List<Step.Finished> delivered = new ArrayList<>();
    private final Partition partition = new Partition("a.com", SETTINGS, DelayPolicy.NONE);

    @Test
    void runsTasksOneAtATimeInOrder() {
        submit(partition, "a");
        submit(partition, "b");
        assertEquals(List.of("a#1"), started());

        run(partition, partition.completed(done("a", 1), T0));
        assertEquals(List.of("a#1", "b#1"), started());
        assertEquals(TaskStatus.DONE, results().getFirst().status());

        run(partition, partition.completed(done("b", 1), T0));
        assertTrue(partition.isIdle(T0));
    }

    @Test
    void commandsDoNotChangeTheState() {
        partition.submit(task("a"), request("a"), T0);
        assertEquals(List.of(new Submitted(task("a"), request("a")), new Started("a", 1, T0 + 60_000)),
            partition.submit(task("a"), request("a"), T0));
    }

    @Test
    void finalResultGoesWithTheStepNotWithTheEvents() {
        submit(partition, "a");
        final Step step = partition.completed(done("a", 1), T0);

        assertEquals(List.of(new PartitionEvent.Ended("a", 1, true, T0)), step.events());
        assertEquals("a", step.finished().result().taskId());
        assertEquals(List.of(request("a")), step.finished().requests());
        assertNull(partition.completed(retry("a", 1, "io"), T0).finished(), "a retry finishes nothing");
    }

    @Test
    void retriesUntilAttemptsAreExhausted() {
        submit(partition, "a");
        run(partition, partition.completed(retry("a", 1, "io"), T0));
        run(partition, partition.completed(retry("a", 2, "io"), T0));
        run(partition, partition.completed(retry("a", 3, "timeout"), T0));

        assertEquals(List.of("a#1", "a#2", "a#3"), started());
        final TaskResult result = results().getFirst();
        assertEquals(TaskStatus.FAILED, result.status());
        assertEquals("attempts exhausted, last error: timeout", result.error());
        assertEquals("3", result.meta().get(TaskQueue.ATTEMPTS));
    }

    @Test
    void failedAttemptFailsTheTaskAtOnce() {
        submit(partition, "a");
        run(partition, partition.completed(AttemptResult.failed(new Attempt(task("a"), 1), "bad url"), T0));

        assertEquals(List.of("a#1"), started());
        assertEquals("bad url", results().getFirst().error());
    }

    @Test
    void sameRequestIsNotQueuedTwiceAndAnotherPayloadIsAnError() {
        submit(partition, "a");
        assertEquals(List.of(), partition.submit(task("a"), request("a"), T0));
        assertThrows(IllegalArgumentException.class,
            () -> partition.submit(new Task("a", Blob.of(new byte[]{9}), Map.of()), request("x"), T0));
    }

    /**
     * Several requests of one task: done once, the result goes to all of them.
     */
    @Test
    void requestsOfTheSameTaskShareItsResult() {
        submit(partition, "a");
        final Request other = new Request(new RequestId("other", "0", "r1"), Map.of("reply-to", "q"));
        assertEquals(List.of(new Joined("a", other)), partition.submit(task("a"), other, T0));
        run(partition, partition.submit(task("a"), other, T0));
        assertEquals(List.of(), partition.submit(task("a"), other, T0), "a retried request joins once");

        run(partition, partition.completed(done("a", 1), T0));

        assertEquals(List.of("a#1"), started());
        assertEquals(List.of(request("a"), other), delivered.getFirst().requests());
    }

    @Test
    void requestWithAnotherTaskIsAnError() {
        submit(partition, "a");
        assertThrows(IllegalArgumentException.class, () -> partition.submit(task("b"), request("a"), T0));
    }

    @Test
    void requestAfterItsTaskFinishedIsANewTask() {
        submit(partition, "a");
        run(partition, partition.completed(done("a", 1), T0));
        submit(partition, "a");

        assertEquals(List.of("a#1", "a#1"), started());
    }

    @Test
    void nextTaskWaitsForThePauseAfterTheResult() {
        final Partition paused = new Partition("a.com", SETTINGS, DelayPolicy.fixed(PAUSE));
        submit(paused, "a");
        submit(paused, "b");
        run(paused, paused.completed(done("a", 1), T0 + 100));

        assertEquals(List.of("a#1"), started());
        assertEquals(T0 + 100 + PAUSE.toMillis(), paused.nextDeadline());
        assertEquals(Step.NONE, paused.tick(T0 + 100 + PAUSE.toMillis() - 1));

        run(paused, paused.tick(T0 + 100 + PAUSE.toMillis()));
        assertEquals(List.of("a#1", "b#1"), started());
    }

    @Test
    void retryWaitsForThePauseToo() {
        final Partition paused = new Partition("a.com", SETTINGS, DelayPolicy.fixed(PAUSE));
        submit(paused, "a");
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
        submit(paused, "a");
        run(paused, paused.completed(done("a", 1), T0));
        assertTrue(paused.isEmpty());
        assertFalse(paused.isIdle(T0), "empty, but the pause lasts");

        run(paused, paused.submit(task("b"), request("b"), T0 + 10));
        assertEquals(List.of("a#1"), started());
        run(paused, paused.tick(T0 + PAUSE.toMillis()));
        assertEquals(List.of("a#1", "b#1"), started());
    }

    @Test
    void independentTaskHasNoPause() {
        final Partition independent = new Partition(null, SETTINGS, DelayPolicy.fixed(PAUSE));
        submit(independent, "a");
        run(independent, independent.completed(done("a", 1), T0));

        assertTrue(independent.isIdle(T0));
    }

    /**
     * No result by the deadline (the worker is gone): the attempt counts as RETRY; its late result is ignored.
     */
    @Test
    void attemptWithoutAResultInTimeIsRetried() {
        submit(partition, "a");
        assertEquals(T0 + TIMEOUT.toMillis(), partition.nextDeadline());
        assertEquals(Step.NONE, partition.tick(T0 + TIMEOUT.toMillis() - 1));

        run(partition, partition.tick(T0 + TIMEOUT.toMillis()));
        assertEquals(List.of("a#1", "a#2"), started());
        assertEquals(Step.NONE, partition.completed(done("a", 1), T0 + TIMEOUT.toMillis() + 1));

        run(partition, partition.completed(done("a", 2), T0 + TIMEOUT.toMillis() + 2));
        assertEquals("2", results().getFirst().meta().get(TaskQueue.ATTEMPTS));
    }

    @Test
    void ignoresResultsOfAttemptsNotInProgress() {
        submit(partition, "a");
        run(partition, partition.completed(retry("a", 1, "io"), T0));

        assertEquals(Step.NONE, partition.completed(done("a", 1), T0));
        assertEquals(Step.NONE, partition.completed(done("other", 2), T0));
    }

    /**
     * The journal replayed into a new partition gives the same state, whatever the clock and the settings are then.
     */
    @Test
    void replayRestoresTheState() {
        final Partition paused = new Partition("a.com", SETTINGS, DelayPolicy.fixed(PAUSE));
        submit(paused, "a");
        submit(paused, "b");
        run(paused, paused.submit(task("b"), request("b2"), T0));
        run(paused, paused.completed(done("a", 1), T0));

        final Partition restored = new Partition("a.com", new QueueSettings(1, Duration.ofSeconds(1)), DelayPolicy.NONE);
        journal.forEach(restored::apply);

        assertEquals(paused.snapshot(), restored.snapshot());
        assertEquals(List.of(new Started("b", 1, T0 + PAUSE.toMillis() + 60_000)), paused.tick(T0 + PAUSE.toMillis()).events());
        assertEquals(paused.tick(T0 + PAUSE.toMillis()).events().size(), restored.tick(T0 + PAUSE.toMillis()).events().size());
    }

    @Test
    void snapshotRestoresTheState() {
        final Partition paused = new Partition("a.com", SETTINGS, DelayPolicy.fixed(PAUSE));
        submit(paused, "a");
        submit(paused, "b");
        submit(paused, "c");
        run(paused, paused.completed(retry("a", 1, "io"), T0));

        final Partition restored = Partition.restore("a.com", SETTINGS, DelayPolicy.fixed(PAUSE), paused.snapshot());

        assertEquals(paused.snapshot(), restored.snapshot());
        assertEquals(List.of(), restored.submit(task("b"), request("b"), T0), "known requests survive the snapshot");
        assertEquals(paused.tick(T0 + PAUSE.toMillis()), restored.tick(T0 + PAUSE.toMillis()));
    }

    private void submit(Partition target, String id) {
        run(target, target.submit(task(id), request(id), T0));
    }

    private void run(Partition target, List<PartitionEvent> events) {
        events.forEach(target::apply);
        journal.addAll(events);
    }

    private void run(Partition target, Step step) {
        if (step.finished() != null) {
            delivered.add(step.finished());
        }
        run(target, step.events());
    }

    private List<String> started() {
        return journal.stream()
            .filter(e -> e instanceof Started)
            .map(e -> ((Started) e).taskId() + "#" + ((Started) e).attempt())
            .toList();
    }

    private List<TaskResult> results() {
        return delivered.stream().map(Step.Finished::result).toList();
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

    private static Request request(String id) {
        return Request.of(new RequestId("client", "0", id));
    }
}
