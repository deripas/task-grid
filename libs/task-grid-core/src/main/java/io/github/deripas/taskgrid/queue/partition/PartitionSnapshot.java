package io.github.deripas.taskgrid.queue.partition;

import io.github.deripas.taskgrid.task.Attempt;
import io.github.deripas.taskgrid.task.Task;
import io.github.deripas.taskgrid.task.TaskResult;

import java.util.List;

/**
 * Immutable copy of the state of a {@link Partition}: a snapshot of an event-sourced engine, so that recovery does not
 * replay the whole history of a key.
 *
 * @param waiting     tasks waiting, in order
 * @param current     the attempt in progress or the retry waiting for the pause; {@code null} - none
 * @param running     {@code current} is handed out
 * @param deadline    of the attempt in progress, epoch ms
 * @param resumeAt    end of the pause, epoch ms
 * @param undelivered final results not delivered yet
 */
public record PartitionSnapshot(
    List<Task> waiting,
    Attempt current,
    boolean running,
    long deadline,
    long resumeAt,
    List<TaskResult> undelivered
) {
    public PartitionSnapshot {
        waiting = List.copyOf(waiting);
        undelivered = List.copyOf(undelivered);
    }
}
