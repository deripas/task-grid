package io.github.deripas.taskgrid.worker;

import io.github.deripas.taskgrid.task.AttemptResult;

import java.util.concurrent.CompletionStage;

/**
 * Receives the results of the attempts a worker accepted ({@link Worker#onComplete}); usually the queue that owns them.
 * Called on a thread of the worker: must not block.
 */
public interface AttemptCallback {
    /**
     * Result of an accepted attempt; reported once per attempt. Completes when the owner has taken the result (a
     * persistent queue - when it is in the journal): only then may the worker forget it.
     */
    CompletionStage<Void> complete(AttemptResult result);
}
