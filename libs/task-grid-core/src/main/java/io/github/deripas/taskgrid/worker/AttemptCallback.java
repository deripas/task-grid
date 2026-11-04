package io.github.deripas.taskgrid.worker;

import io.github.deripas.taskgrid.task.AttemptResult;

/**
 * What a worker reports to the owner of attempts (the queue). Implemented by the queue, called by the worker.
 */
public interface AttemptCallback {
    /**
     * Result of an accepted attempt; reported once per attempt.
     */
    void complete(AttemptResult result);
}
