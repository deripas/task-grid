package io.github.deripas.taskgrid.worker;

import io.github.deripas.taskgrid.task.AttemptResult;

/**
 * Receives the results of the attempts a worker accepted ({@link Worker#onComplete}); usually the queue that owns them.
 * Called on a thread of the worker: must not block.
 */
public interface AttemptCallback {
    /**
     * Result of an accepted attempt; reported once per attempt.
     */
    void complete(AttemptResult result);
}
