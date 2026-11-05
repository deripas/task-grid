package io.github.deripas.crawler.urlmanager.app;

import io.github.deripas.taskgrid.queue.TaskQueue;
import io.github.deripas.taskgrid.worker.remote.RemoteWorker;

/**
 * The task-grid node: the queue (in memory or in the cluster) and the remote worker (sidecars), connected by
 * {@code Dispatcher}.
 */
record Node(
    TaskQueue queue,
    RemoteWorker remote
) {
}
