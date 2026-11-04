package io.github.deripas.taskgrid.task;

import java.util.Map;
import java.util.Objects;

/**
 * Final result of a task.
 *
 * @param body  output of the handler when {@link TaskStatus#DONE}, otherwise {@code null}
 * @param error description of the failure when {@link TaskStatus#FAILED}, otherwise {@code null}
 * @param meta  meta of the task plus keys added on the way (e.g. {@code TaskQueue.ATTEMPTS}); immutable copy
 */
public record TaskResult(
    String taskId,
    TaskStatus status,
    Blob body,
    String error,
    Map<String, String> meta
) {
    public TaskResult {
        Objects.requireNonNull(taskId, "taskId");
        Objects.requireNonNull(status, "status");
        meta = Map.copyOf(meta);
    }
}
