package io.github.deripas.crawler.urlmanager.app;

import io.github.deripas.taskgrid.queue.memory.MemoryTaskQueue;
import io.github.deripas.taskgrid.worker.remote.RemoteWorker;

/**
 * The in-memory task-grid node: the queue (crawl-delay per host) and the remote worker (sidecars), connected by
 * {@code Dispatcher}.
 */
record Node(
    MemoryTaskQueue queue,
    RemoteWorker remote
) implements AutoCloseable {
    @Override
    public void close() {
        queue.close();
        remote.close();
    }
}
