package io.github.deripas.taskgrid.queue;

import io.github.deripas.taskgrid.task.Request;
import io.github.deripas.taskgrid.task.TaskResult;

import java.util.List;
import java.util.concurrent.CompletionStage;

/**
 * Receives final results of the tasks of a queue, on the node where a task finished, with the requests the task
 * served: the way to answer clients that are not waiting on this node.
 * <p>
 * Called on a thread of the queue: must not block - hand the work over and complete the stage when it is done.
 * How often a result is delivered depends on the queue: an in-memory queue calls once and forgets the result even
 * if the stage fails; a persistent queue finishes the task only once all the callbacks completed normally and
 * delivers again otherwise (at least once).
 */
public interface ResultCallback {
    CompletionStage<Void> finished(TaskResult result, List<Request> requests);
}
