package io.github.deripas.taskgrid.worker.remote;

import io.github.deripas.taskgrid.task.AttemptResult;

import java.util.concurrent.CompletionStage;

/**
 * Port for the transport adapter: sessions of sidecars with this node and their results. The adapter translates
 * the protocol; attempts and demand come back through {@link SessionChannel}. Calls do not block.
 */
public interface Sidecars {
    /**
     * Opens a session of a sidecar. A previous session of the same worker id is superseded as the channel; attempts in
     * progress belong to the process and stay, unless its instance id has changed.
     */
    Session connect(SidecarHello hello, SessionChannel channel);

    /**
     * Result of an attempt executed by the sidecar; may come through any of its sessions, after they have closed,
     * or through another node. Completes when the result
     * is taken (a persistent queue - in its journal): the sidecar may forget it then, otherwise it sends it again.
     */
    CompletionStage<Void> complete(String workerId, AttemptResult result);
}
