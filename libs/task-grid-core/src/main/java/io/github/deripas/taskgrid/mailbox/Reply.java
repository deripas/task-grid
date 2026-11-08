package io.github.deripas.taskgrid.mailbox;

import io.github.deripas.taskgrid.task.TaskResult;

import java.util.Objects;

/**
 * A result in a box, under the id the client gave its request (the result itself is of the task of the queue, which
 * may serve several requests).
 */
public record Reply(
    String id,
    TaskResult result
) {
    public Reply {
        Objects.requireNonNull(id, "id");
        Objects.requireNonNull(result, "result");
    }
}
