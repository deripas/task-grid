package io.github.deripas.taskgrid.worker.remote;

import io.github.deripas.taskgrid.task.AttemptResult;

/**
 * Port for the transport adapter: sessions of sidecars with this node and their results. The adapter translates
 * the protocol; attempts and demand come back through {@link SessionChannel}. Calls do not block.
 */
public interface Sidecars {
    /**
     * Opens a session of a sidecar. A previous session of the same worker id is superseded.
     */
    Session connect(
        String workerId,
        String kind,
        int capacity,
        SessionChannel channel
    );

    /**
     * Result of an attempt executed by the sidecar; may come after its session has closed.
     */
    void complete(String workerId, AttemptResult result);
}
