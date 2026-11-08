package io.github.deripas.taskgrid.gateway;

import io.github.deripas.taskgrid.task.TaskResult;

/**
 * What became of a request waited for on this node ({@link Gateway#call}).
 */
public sealed interface Call {
    record Done(TaskResult result) implements Call {
    }

    /**
     * No result within the timeout; the task may still finish, its result is dropped then.
     */
    record TimedOut() implements Call {
    }

    /**
     * Not submitted: the box of the client on this node is full.
     */
    record Busy() implements Call {
    }

    /**
     * Not submitted: refused by the queue (its id is there with another payload).
     */
    record Rejected(String reason) implements Call {
    }
}
