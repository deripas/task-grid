package io.github.deripas.taskgrid.worker.remote;

/**
 * Who opens a session with the node.
 *
 * @param workerId   stable name of the sidecar (pod name): survives a restart of the process
 * @param instanceId random value of the process, new on each start: another value under the same worker id - the
 *                   process was restarted and has lost its attempts in progress
 * @param kind       kind of tasks the handler executes
 * @param capacity   attempts the handler runs at once
 */
public record SidecarHello(
    String workerId,
    String instanceId,
    String kind,
    int capacity
) {
}
