package io.github.deripas.taskgrid.result;

import io.github.deripas.taskgrid.task.TaskResult;

import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.CompletionStage;

/**
 * Final results of tasks by task id until the client takes them. The queue puts results, the entry point reads them.
 */
public interface ResultStore {
    CompletionStage<Void> put(TaskResult result);

    /**
     * Completes with the result as soon as it is put, or empty if it is not there within the timeout.
     */
    CompletionStage<Optional<TaskResult>> await(String taskId, Duration timeout);
}
