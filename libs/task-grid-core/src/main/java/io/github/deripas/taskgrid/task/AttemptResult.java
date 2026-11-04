package io.github.deripas.taskgrid.task;

import java.util.Objects;

/**
 * Result of one attempt.
 *
 * @param attempt attempt number, from 1
 * @param body    output of the handler when {@link AttemptStatus#DONE}, otherwise {@code null}
 * @param error   description of the failure when not {@link AttemptStatus#DONE}, otherwise {@code null}
 */
public record AttemptResult(
    String taskId,
    int attempt,
    AttemptStatus status,
    Blob body,
    String error
) {
    public AttemptResult {
        Objects.requireNonNull(taskId, "taskId");
        Objects.requireNonNull(status, "status");
    }
}
