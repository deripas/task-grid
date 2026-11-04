package io.github.deripas.crawler.urlmanager.app;

import io.github.deripas.taskgrid.queue.memory.MemoryTaskQueue;
import io.github.deripas.taskgrid.worker.remote.RemoteWorker;

/**
 * The in-memory task-grid node: the queue creates its worker, so both come out of one factory.
 */
record Node(
    MemoryTaskQueue queue,
    RemoteWorker worker
) implements AutoCloseable {
    @Override
    public void close() {
        worker.close();
    }
}
