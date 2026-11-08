package io.github.deripas.crawler.urlmanager.fetch;

import io.github.deripas.taskgrid.gateway.Call;
import io.github.deripas.taskgrid.gateway.Gateway;
import io.github.deripas.taskgrid.queue.TaskQueue;
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
import java.util.HashMap;
import java.util.Map;

/**
 * Fetch of one page as a task of task-grid: kind {@value #KIND}, the payload is the request to the fetcher, so the
 * same page asked for by several clients at once is fetched once; partition key - the host, so fetches of a host run
 * one at a time, in order, with a pause; affinity key - the host too, so they go to the same fetcher (its connections
 * to the site stay warm).
 */
@Slf4j
@RequiredArgsConstructor
public final class FetchService {
    /**
     * Kind of the tasks: the handler-sidecar of the fetcher is started with {@code KIND=fetch}.
     */
    public static final String KIND = "fetch";

    private final Gateway gateway;
    private final PageCodec codec;
    private final BlobReader blobs;

    /**
     * Fetches the page for a client waiting on this node: its box of this node holds the request until the result
     * is taken, a full box - {@link FetchOutcome.Busy}. The request is the page: the same page asked for again while
     * it is fetched is the same request.
     *
     * @param client the box of the client on this node
     */
    public FetchOutcome fetch(
        String client,
        PageUrl url,
        Duration timeout
    ) throws IOException {
        final Task task = task(url);
        return switch (gateway.call(client, task.id(), task, timeout).toCompletableFuture().join()) {
            case Call.Done done -> outcome(done.result());
            case Call.TimedOut ignored -> {
                log.info("Fetch timed out: client={} url={} timeout={}", client, url.uri(), timeout);
                yield new FetchOutcome.TimedOut();
            }
            case Call.Busy ignored -> {
                log.info("Fetch refused, the box is full: client={} url={}", client, url.uri());
                yield new FetchOutcome.Busy();
            }
            case Call.Rejected rejected -> new FetchOutcome.Failed(rejected.reason());
        };
    }

    /**
     * The task of a fetch: the same URL is the same task while it is queued.
     */
    public Task task(PageUrl url) {
        final Map<String, String> meta = new HashMap<>();
        meta.put(Worker.KIND, KIND);
        // one fetch of a host at a time, with its crawl-delay; on the same fetcher instance: warm connections
        meta.put(TaskQueue.PARTITION_KEY, url.host());
        meta.put(Worker.AFFINITY_KEY, url.host());
        return Task.of(Blob.of(codec.request(url)), meta);
    }

    /**
     * The outcome of a finished fetch: a page may be read from blob storage, so it blocks; the page must be closed.
     */
    public FetchOutcome outcome(TaskResult result) throws IOException {
        return switch (result.status()) {
            case DONE -> new FetchOutcome.Fetched(codec.page(open(result.body())));
            case FAILED -> {
                log.info("Fetch failed: task={} error={}", result.taskId(), result.error());
                yield new FetchOutcome.Failed(result.error());
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
