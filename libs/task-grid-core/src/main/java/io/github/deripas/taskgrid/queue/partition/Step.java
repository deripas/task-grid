package io.github.deripas.taskgrid.queue.partition;

import io.github.deripas.taskgrid.task.Request;
import io.github.deripas.taskgrid.task.TaskResult;

import java.util.List;

/**
 * Decision of a command of a {@link Partition}: the events, and the final result of the task in progress if it
 * finishes. A persistent engine delivers the result to the requests first and persists the events only then: the
 * partition keeps no results, and a delivery that failed is decided again (the attempt is still in progress in the
 * journal - its result comes again, or its deadline passes again).
 *
 * @param finished {@code null} - no task finishes
 */
public record Step(
    List<PartitionEvent> events,
    Finished finished
) {
    public static final Step NONE = new Step(List.of(), null);

    public Step {
        events = List.copyOf(events);
    }

    /**
     * The final result of a task and the requests to deliver it to.
     */
    public record Finished(
        TaskResult result,
        List<Request> requests
    ) {
        public Finished {
            requests = List.copyOf(requests);
        }
    }
}
