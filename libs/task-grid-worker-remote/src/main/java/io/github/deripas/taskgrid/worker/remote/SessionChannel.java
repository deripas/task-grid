package io.github.deripas.taskgrid.worker.remote;

import io.github.deripas.taskgrid.task.Attempt;

/**
 * Port to the transport of a session: what the node writes to the sidecar. Implemented by the transport adapter.
 * Methods of one channel are called sequentially, never concurrently; they do not block.
 */
public interface SessionChannel {
    /**
     * Sends an attempt with an inline payload. {@code false} - not sent (the stream is closed): the attempt goes
     * to another sidecar.
     */
    boolean attempt(Attempt attempt);

    /**
     * Sends the demand of the node. {@code false} - not sent (the stream is closed).
     */
    boolean demand(long target, long version);

    /**
     * The session is closed by the node (drained, superseded, node stopping): close the stream.
     */
    void close();
}
