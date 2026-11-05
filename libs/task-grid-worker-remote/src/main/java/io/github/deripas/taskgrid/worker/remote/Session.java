package io.github.deripas.taskgrid.worker.remote;

/**
 * Session of a sidecar as seen by the transport adapter: what the sidecar tells the node. Calls do not block.
 */
public interface Session {
    /**
     * The node may send attempts with session numbers up to {@code limit}.
     */
    void credit(long limit, long version);

    /**
     * Share of the capacity of the sidecar this node counts in its load (the sidecar splits it among its nodes).
     */
    void share(double capacity);

    /**
     * The sidecar stops (end of its side of the stream): no more attempts are sent to it; the session ends once
     * the results of its attempts in progress have come.
     */
    void drain();

    /**
     * The transport is broken on the sidecar side (cancellation, error, protocol violation): the session ends now;
     * its attempts in progress wait for the sidecar to reconnect (see {@link RemoteSettings#rejoinTimeout()}).
     */
    void closed(String reason);
}
