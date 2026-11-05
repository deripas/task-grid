package io.github.deripas.crawler.urlmanager.fetch;

import io.github.deripas.taskgrid.dispatch.Dispatcher;
import io.github.deripas.taskgrid.queue.DelayPolicy;
import io.github.deripas.taskgrid.queue.QueueSettings;
import io.github.deripas.taskgrid.queue.memory.MemoryTaskQueue;
import io.github.deripas.taskgrid.result.ResultStore;
import io.github.deripas.taskgrid.result.memory.MemoryResultStore;
import io.github.deripas.taskgrid.task.Attempt;
import io.github.deripas.taskgrid.task.Blob;
import io.github.deripas.taskgrid.worker.local.Handler;
import io.github.deripas.taskgrid.worker.local.LocalWorker;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * FetchService on the in-memory node of task-grid with a handler in the JVM instead of the fetcher behind a sidecar.
 * Protocol of the tests: the payload is the URL, the result is the page body; status 200, no headers.
 */
public final class LocalFetch implements AutoCloseable {
    private final ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor();
    private final List<Attempt> attempts = new CopyOnWriteArrayList<>();
    private final MemoryTaskQueue queue;
    private final FetchService service;

    /**
     * @param handler answers attempts of the fetch kind
     * @param blobs   reads results the handler returns as references
     */
    public LocalFetch(Handler handler, BlobReader blobs) {
        final ResultStore results = new MemoryResultStore(Duration.ofMinutes(1), Clock.systemUTC());
        final Handler recording = attempt -> {
            attempts.add(attempt);
            return handler.handle(attempt);
        };
        queue = new MemoryTaskQueue(new QueueSettings(2), DelayPolicy.NONE, results);
        Dispatcher.connect(queue, new LocalWorker(Map.of(FetchService.KIND, recording), executor));
        this.service = new FetchService(queue, results, new PageCodec() {
            @Override
            public byte[] request(PageUrl url) {
                return url.uri().toString().getBytes(StandardCharsets.UTF_8);
            }

            @Override
            public Page page(InputStream result) {
                return new Page(200, List.of(), -1, result);
            }
        }, blobs);
    }

    public static String url(Attempt attempt) {
        return new String(((Blob.Inline) attempt.task().payload()).bytes(), StandardCharsets.UTF_8);
    }

    public FetchService service() {
        return service;
    }

    public List<Attempt> attempts() {
        return attempts;
    }

    @Override
    public void close() {
        executor.shutdownNow();
        queue.close();
    }

    /**
     * Content of a page in the tests.
     */
    public static String text(Page page) throws IOException {
        try (page) {
            return new String(page.body().readAllBytes(), StandardCharsets.UTF_8);
        }
    }
}
