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
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

/**
 * Fetch of one page as a task of task-grid: kind {@value #KIND}, affinity key - the host, so fetches of a host run
 * one at a time, in order. The result is either waited for here, within the timeout of the client, or delivered
 * to a {@link FetchCallback} on the node where the task finished, together with the tags given on submit.
 */
@Slf4j
@RequiredArgsConstructor
public final class FetchService {
    /**
     * Kind of the tasks: the handler-sidecar of the fetcher is started with {@code KIND=fetch}.
     */
    public static final String KIND = "fetch";

    /**
     * Prefix of the tags in the task meta: they stay apart from the keys of task-grid.
     */
    private static final String TAG = "tag.";

    private final TaskQueue queue;
    private final ResultStore results;
    private final PageCodec codec;
    private final BlobReader blobs;

    /**
     * Submits a fetch with a new id and waits for its page.
     */
    public FetchOutcome fetch(PageUrl url, Duration timeout) throws IOException {
        final String id = UUID.randomUUID().toString();
        submit(id, url, Map.of());
        return result(id, url, timeout);
    }

    /**
     * Queues the fetch; returns when the queue has taken it. The same id while its task is still queued is the same
     * fetch (a request delivered again): it is not queued twice.
     *
     * @param tags small strings that travel with the task to its {@link FinishedFetch}, e.g. the reply address
     */
    public void submit(
        String id,
        PageUrl url,
        Map<String, String> tags
    ) {
        final Map<String, String> meta = new HashMap<>();
        tags.forEach((key, value) -> meta.put(TAG + key, value));
        meta.put(Worker.KIND, KIND);
        meta.put(Task.AFFINITY_KEY, url.host());
        queue.submit(new Task(id, Blob.of(codec.request(url)), meta)).toCompletableFuture().join();
    }

    /**
     * Waits for the result of a submitted fetch.
     */
    public FetchOutcome result(
        String id,
        PageUrl url,
        Duration timeout
    ) throws IOException {
        final Optional<TaskResult> result = results.await(id, timeout).toCompletableFuture().join();
        if (result.isEmpty()) {
            log.info("Fetch timed out: task={} url={} timeout={}", id, url.uri(), timeout);
            return new FetchOutcome.TimedOut();
        }
        return outcome(result.get());
    }

    /**
     * Registers a receiver of every finished fetch; see {@link FetchCallback} for the threads and the delivery.
     */
    public void onFinished(FetchCallback callback) {
        queue.onResult(result -> {
            if (!KIND.equals(result.meta().get(Worker.KIND))) {
                return CompletableFuture.completedFuture(null);
            }
            return callback.finished(new FinishedFetch(result.taskId(), tags(result.meta()), () -> outcome(result)));
        });
    }

    private FetchOutcome outcome(TaskResult result) throws IOException {
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

    private static Map<String, String> tags(Map<String, String> meta) {
        final Map<String, String> tags = new HashMap<>();
        meta.forEach((key, value) -> {
            if (key.startsWith(TAG)) {
                tags.put(key.substring(TAG.length()), value);
            }
        });
        return tags;
    }
}
