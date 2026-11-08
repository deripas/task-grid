package io.github.deripas.taskgrid.queue;

import io.github.deripas.taskgrid.task.AttemptResult;
import io.github.deripas.taskgrid.task.Request;
import io.github.deripas.taskgrid.task.Task;

import java.util.concurrent.CompletionStage;

/**
 * Tasks with their order, retries and pauses, independent of who executes them. Requests come in through
 * {@link #submit}; attempts the queue has ready go out to {@link ReadyCallback}s; their results come back through
 * {@link #complete}; final results go to the {@link ResultCallback}s with the requests of the task.
 * <p>
 * The queue keeps the tasks, not the results: a result leaves the queue through the callbacks.
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
     * Completes when the queue has accepted the request.
     * <ul>
     *   <li>A task whose id is in the queue is not queued again: the request joins it and gets its result; another
     *   payload under the id is an {@link IllegalArgumentException}.</li>
     *   <li>The same request again (a retried submission) changes nothing; the id of a request in the queue with
     *   another task is an {@link IllegalArgumentException}.</li>
     * </ul>
     */
    CompletionStage<Void> submit(Task task, Request request);

    /**
     * Result of an attempt the queue handed out. Completes when the queue has taken it (a persistent queue - when
     * a final result is delivered and the end is in the journal); a result of an attempt not in progress (late,
     * duplicate) is ignored and completes too.
     */
    CompletionStage<Void> complete(AttemptResult result);

    /**
     * Registers a receiver of the attempts ready to run. Register before submitting tasks: an attempt handed out while
     * there is no receiver waits for its result forever.
     */
    void onReady(ReadyCallback callback);

    /**
     * Registers a receiver of the final results of all tasks. Register before submitting tasks; results finished
     * earlier are not delivered.
     */
    void onResult(ResultCallback callback);
}
