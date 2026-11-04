package io.github.deripas.taskgrid.queue;

import io.github.deripas.taskgrid.task.Task;

import java.util.concurrent.CompletionStage;

/**
 * Entry point for tasks. The final result of a task is put into the result store under the task id.
 */
public interface TaskQueue {
    /**
     * Result meta: number of attempts made.
     */
    String ATTEMPTS = "attempts";

    /**
     * Completes when the queue has accepted the task.
     */
    CompletionStage<Void> submit(Task task);
}
