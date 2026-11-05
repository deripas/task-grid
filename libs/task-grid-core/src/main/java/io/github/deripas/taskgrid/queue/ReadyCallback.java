package io.github.deripas.taskgrid.queue;

import io.github.deripas.taskgrid.task.Attempt;

/**
 * Receives the attempts a queue has ready to run: its partition has nothing in progress and its pause is over.
 * The result of each attempt goes back to {@link TaskQueue#complete}; until then its partition waits.
 * <p>
 * Called on a thread of the queue, possibly under its lock: must not block - hand the attempt over and return.
 */
public interface ReadyCallback {
    void ready(Attempt attempt);
}
