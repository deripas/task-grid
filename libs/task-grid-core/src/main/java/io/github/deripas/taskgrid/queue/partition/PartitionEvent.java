package io.github.deripas.taskgrid.queue.partition;

import io.github.deripas.taskgrid.task.Attempt;
import io.github.deripas.taskgrid.task.Task;
import io.github.deripas.taskgrid.task.TaskResult;

/**
 * What happened to a partition. Decisions are inside the events (deadlines, the final result, the end of the pause):
 * replaying them gives the same state whatever the settings and the clock are by then.
 */
public sealed interface PartitionEvent {
    /**
     * The task joined the end of the partition.
     */
    record Submitted(Task task) implements PartitionEvent {
    }

    /**
     * The attempt is handed out; without a result by {@code deadline} (epoch ms) it counts as RETRY.
     */
    record Started(
        Attempt attempt,
        long deadline
    ) implements PartitionEvent {
    }

    /**
     * The attempt in progress ended. {@code result} - the task is finished; {@code null} - its next attempt waits.
     * The next attempt of the partition goes out not earlier than {@code resumeAt} (epoch ms).
     */
    record Ended(
        String taskId,
        int attempt,
        TaskResult result,
        long resumeAt
    ) implements PartitionEvent {
    }

    /**
     * The final result of the task reached its receivers.
     */
    record Delivered(String taskId) implements PartitionEvent {
    }
}
