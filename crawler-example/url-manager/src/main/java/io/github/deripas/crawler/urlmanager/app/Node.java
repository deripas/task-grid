package io.github.deripas.crawler.urlmanager.app;

import io.github.deripas.taskgrid.queue.memory.MemoryTaskQueue;
import io.github.deripas.taskgrid.worker.delayed.DelayedWorker;
import io.github.deripas.taskgrid.worker.remote.RemoteWorker;

/**
 * The in-memory task-grid node: queue -> delayed worker (crawl-delay per host) -> remote worker (sidecars).
 * Each creates the next one with itself as the callback, so all come out of one factory.
 */
record Node(
    MemoryTaskQueue queue,
    DelayedWorker delayed,
    RemoteWorker remote
) implements AutoCloseable {
    @Override
    public void close() {
        delayed.close();
        remote.close();
    }
}
