package io.github.deripas.taskgrid.queue;

import io.github.deripas.taskgrid.task.TaskResult;

import java.util.concurrent.CompletionStage;

/**
 * Receives final results of the tasks of a queue, on the node where a task finished: the way to answer a client
 * that is not waiting on this node (reply address in the task meta).
 * <p>
 * Called on a thread of the queue: must not block - hand the work over and complete the stage when it is done.
 * How often a result is delivered depends on the queue: an in-memory queue calls once and forgets the result even
 * if the stage fails; a persistent queue delivers again until the stage completes normally (at least once).
 */
public interface ResultCallback {
    CompletionStage<Void> finished(TaskResult result);
}
