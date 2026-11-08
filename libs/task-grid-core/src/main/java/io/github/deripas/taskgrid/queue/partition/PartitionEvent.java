package io.github.deripas.taskgrid.queue.partition;

import io.github.deripas.taskgrid.task.Request;
import io.github.deripas.taskgrid.task.Task;

/**
 * What happened to a partition. Decisions are inside the events (deadlines, the end of the pause): replaying them
 * gives the same state whatever the settings and the clock are by then. Results are not here: a final result leaves
 * the partition before its {@link Ended} is written.
 */
public sealed interface PartitionEvent {
    /**
     * The task joined the end of the partition, asked for by the request.
     */
    record Submitted(
        Task task,
        Request request
    ) implements PartitionEvent {
    }

    /**
     * Another request of a task already in the partition: it gets the same result.
     */
    record Joined(
        String taskId,
        Request request
    ) implements PartitionEvent {
    }

    /**
     * The attempt is handed out; without a result by {@code deadline} (epoch ms) it counts as RETRY. Attempt 1 is
     * of the first waiting task, a later one - of the task in progress.
     */
    record Started(
        String taskId,
        int attempt,
        long deadline
    ) implements PartitionEvent {
    }

    /**
     * The attempt in progress ended: {@code finished} - the task leaves the partition (its result is delivered),
     * otherwise its next attempt waits. The next attempt of the partition goes out not earlier than {@code resumeAt}
     * (epoch ms).
     */
    record Ended(
        String taskId,
        int attempt,
        boolean finished,
        long resumeAt
    ) implements PartitionEvent {
    }
}
