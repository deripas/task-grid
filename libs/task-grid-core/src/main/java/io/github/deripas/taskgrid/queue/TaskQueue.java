package io.github.deripas.taskgrid.queue;

import io.github.deripas.taskgrid.task.AttemptResult;
import io.github.deripas.taskgrid.task.Task;

import java.util.concurrent.CompletionStage;

/**
 * Tasks with their order, retries and pauses, independent of who executes them. Tasks come in through {@link #submit};
 * attempts the queue has ready go out to {@link ReadyCallback}s; their results come back through {@link #complete};
 * final results go to the result store under the task id and to {@link ResultCallback}s.
 * <p>
 * Who executes the attempts is not the queue's business: {@code Dispatcher} connects a queue to a worker; a client
 * may also take attempts and complete them itself.
 */
public interface TaskQueue {
    /**
     * Result meta: number of attempts made.
     */
    String ATTEMPTS = "attempts";

    /**
     * Task meta: tasks with the same value run one at a time, in order, with a {@link DelayPolicy pause} between
     * attempts - one partition of the queue (for crawling - a host: politeness, crawl-delay). Absent - the task is
     * independent. Where the attempts run is {@link io.github.deripas.taskgrid.worker.Worker#AFFINITY_KEY}.
     */
    String PARTITION_KEY = "partition-key";

    /**
     * Completes when the queue has accepted the task.
     */
    CompletionStage<Void> submit(Task task);

    /**
     * Result of an attempt the queue handed out. A result of an attempt not in progress (late, duplicate) is ignored.
     */
    void complete(AttemptResult result);

    /**
     * Registers a receiver of the attempts ready to run. Register before submitting tasks: an attempt handed out while
     * there is no receiver waits for its result forever.
     */
    void onReady(ReadyCallback callback);

    /**
     * Registers a receiver of the final results of all tasks, in addition to the result store. Register before
     * submitting tasks; results finished earlier are not delivered.
     */
    void onResult(ResultCallback callback);
}
