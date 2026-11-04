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

    public static AttemptResult done(Attempt attempt, Blob body) {
        return new AttemptResult(attempt.task().id(), attempt.number(), AttemptStatus.DONE, Objects.requireNonNull(body, "body"), null);
    }

    public static AttemptResult failed(Attempt attempt, String error) {
        return new AttemptResult(attempt.task().id(), attempt.number(), AttemptStatus.FAILED, null, error);
    }

    public static AttemptResult retry(Attempt attempt, String error) {
        return new AttemptResult(attempt.task().id(), attempt.number(), AttemptStatus.RETRY, null, error);
    }
}
