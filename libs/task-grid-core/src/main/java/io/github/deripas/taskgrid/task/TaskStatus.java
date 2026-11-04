package io.github.deripas.taskgrid.task;

/**
 * Final status of a task for the client.
 */
public enum TaskStatus {
    /**
     * An attempt produced a result.
     */
    DONE,

    /**
     * Final failure of an attempt, or attempts exhausted.
     */
    FAILED
}
