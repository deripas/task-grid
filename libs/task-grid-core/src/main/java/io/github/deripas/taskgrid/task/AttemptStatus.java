package io.github.deripas.taskgrid.task;

/**
 * How an attempt ended, as reported by the handler. What happens next (retry, final result) is decided by the queue.
 */
public enum AttemptStatus {
    /**
     * There is a result. Domain errors (e.g. HTTP 404) are DONE too: they are content of the result.
     */
    DONE,

    /**
     * Final failure, no retry.
     */
    FAILED,

    /**
     * Technical failure: the attempt counts, the task is retried while attempts remain.
     */
    RETRY
}
