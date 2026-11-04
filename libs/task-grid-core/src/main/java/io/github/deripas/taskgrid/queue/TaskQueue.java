package io.github.deripas.taskgrid.queue;

import io.github.deripas.taskgrid.task.Task;

import java.util.concurrent.CompletionStage;

/**
 * Entry point for tasks. The final result of a task is put into the result store under the task id.
 */
public interface TaskQueue {
    /**
     * Task meta: tasks with the same value run one at a time, in order, preferably on the same handler instance.
     * Absent - the task is independent.
     */
    String AFFINITY_KEY = "affinity-key";

    /**
     * Result meta: number of attempts made.
     */
    String ATTEMPTS = "attempts";

    /**
     * Completes when the queue has accepted the task.
     */
    CompletionStage<Void> submit(Task task);
}
