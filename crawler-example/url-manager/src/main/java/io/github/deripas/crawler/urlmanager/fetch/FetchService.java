package io.github.deripas.crawler.urlmanager.fetch;

import io.github.deripas.taskgrid.queue.TaskQueue;
import io.github.deripas.taskgrid.result.ResultStore;
import io.github.deripas.taskgrid.task.Blob;
import io.github.deripas.taskgrid.task.Task;
import io.github.deripas.taskgrid.task.TaskResult;
import io.github.deripas.taskgrid.worker.Worker;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.time.Duration;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

/**
 * Fetch of one page as a task of task-grid: kind {@value #KIND}, affinity key - the host, so fetches of a host run
 * one at a time, in order. Waits for the result within the timeout of the client.
 */
@Slf4j
@RequiredArgsConstructor
public final class FetchService {
    /**
     * Kind of the tasks: the handler-sidecar of the fetcher is started with {@code KIND=fetch}.
     */
    public static final String KIND = "fetch";

    private final TaskQueue queue;
    private final ResultStore results;
    private final PageCodec codec;
    private final BlobReader blobs;

    public FetchOutcome fetch(PageUrl url, Duration timeout) throws IOException {
        final String id = UUID.randomUUID().toString();
        final Task task = new Task(id, Blob.of(codec.request(url)), Map.of(Worker.KIND, KIND, Task.AFFINITY_KEY, url.host()));
        queue.submit(task).toCompletableFuture().join();
        final Optional<TaskResult> result = results.await(id, timeout).toCompletableFuture().join();
        if (result.isEmpty()) {
            log.info("Fetch timed out: task={} url={} timeout={}", id, url.uri(), timeout);
            return new FetchOutcome.TimedOut();
        }
        final TaskResult done = result.get();
        return switch (done.status()) {
            case DONE -> new FetchOutcome.Fetched(codec.page(open(done.body())));
            case FAILED -> {
                log.info("Fetch failed: task={} url={} error={}", id, url.uri(), done.error());
                yield new FetchOutcome.Failed(done.error());
            }
        };
    }

    private InputStream open(Blob body) throws IOException {
        return switch (body) {
            case Blob.Inline inline -> new ByteArrayInputStream(inline.bytes());
            case Blob.Ref ref -> blobs.open(ref);
        };
    }
}
