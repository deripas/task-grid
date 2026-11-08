package io.github.deripas.taskgrid.queue.partition;

import io.github.deripas.taskgrid.task.Request;
import io.github.deripas.taskgrid.task.Task;

import java.util.List;

/**
 * Immutable copy of the state of a {@link Partition}: a snapshot of an event-sourced engine, so that recovery does not
 * replay the whole history of a key.
 *
 * @param waiting  tasks waiting, in order
 * @param current  the task in progress or retrying after the pause; {@code null} - none
 * @param attempt  number of the attempt of {@code current} in progress or next
 * @param running  the attempt of {@code current} is handed out
 * @param deadline of the attempt in progress, epoch ms
 * @param resumeAt end of the pause, epoch ms
 */
public record PartitionSnapshot(
    List<Work> waiting,
    Work current,
    int attempt,
    boolean running,
    long deadline,
    long resumeAt
) {
    public PartitionSnapshot {
        waiting = List.copyOf(waiting);
    }

    /**
     * A task and its requests.
     */
    public record Work(
        Task task,
        List<Request> requests
    ) {
        public Work {
            requests = List.copyOf(requests);
        }
    }
}
