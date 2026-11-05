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
     * Task meta: tasks with the same value run one at a time, in order - one partition of the queue (for crawling -
     * a domain: politeness, crawl-delay). Absent - the task is independent. Where the attempts run is
     * {@link io.github.deripas.taskgrid.worker.Worker#AFFINITY_KEY}.
     */
    String PARTITION_KEY = "partition-key";

    /**
     * Completes when the queue has accepted the task.
     */
    CompletionStage<Void> submit(Task task);

    /**
     * Registers a receiver of the final results of all tasks, in addition to the result store. Register before
     * submitting tasks; results finished earlier are not delivered.
     */
    void onResult(ResultCallback callback);
}
