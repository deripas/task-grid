package io.github.deripas.crawler.urlmanager.fetch;

import io.github.deripas.taskgrid.queue.TaskQueue;
import io.github.deripas.taskgrid.task.Attempt;
import io.github.deripas.taskgrid.task.Blob;
import io.github.deripas.taskgrid.worker.Worker;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.concurrent.CountDownLatch;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;

class FetchServiceTest {
    private static final Duration WAIT = Duration.ofSeconds(5);

    private LocalFetch fetch;

    @AfterEach
    void stop() {
        fetch.close();
    }

    @Test
    void pageOfTheHandlerIsFetched() throws IOException {
        fetch = new LocalFetch(attempt -> Blob.of(("page of " + LocalFetch.url(attempt)).getBytes(StandardCharsets.UTF_8)), noBlobs());

        final FetchOutcome outcome = fetch.service().fetch("test", PageUrl.parse("https://Example.com/a"), WAIT);

        assertEquals("page of https://Example.com/a", LocalFetch.text(assertInstanceOf(FetchOutcome.Fetched.class, outcome).page()));
        final Attempt attempt = fetch.attempts().getFirst();
        assertEquals(FetchService.KIND, attempt.task().meta().get(Worker.KIND));
        assertEquals("example.com", attempt.task().meta().get(TaskQueue.PARTITION_KEY));
        assertEquals("example.com", attempt.task().meta().get(Worker.AFFINITY_KEY));
    }

    @Test
    void referenceIsReadFromBlobStorage() throws IOException {
        final Blob.Ref ref = new Blob.Ref("s3://results/t/1", 5, "hash", "zstd");
        fetch = new LocalFetch(attempt -> ref, stored -> {
            assertEquals(ref, stored);
            return new ByteArrayInputStream("large".getBytes(StandardCharsets.UTF_8));
        });

        final FetchOutcome outcome = fetch.service().fetch("test", PageUrl.parse("https://example.com/sitemap.xml"), WAIT);

        assertEquals("large", LocalFetch.text(assertInstanceOf(FetchOutcome.Fetched.class, outcome).page()));
    }

    @Test
    void failureOfTheHandlerFailsTheFetch() throws IOException {
        fetch = new LocalFetch(attempt -> {
            throw new IllegalStateException("site unreachable");
        }, noBlobs());

        final FetchOutcome outcome = fetch.service().fetch("test", PageUrl.parse("https://example.com/"), WAIT);

        final String error = assertInstanceOf(FetchOutcome.Failed.class, outcome).error();
        assertTrue(error.contains("site unreachable"), error);
    }

    @Test
    void noResultWithinTheTimeout() throws IOException {
        fetch = new LocalFetch(attempt -> {
            Thread.sleep(2_000);
            return Blob.of(new byte[0]);
        }, noBlobs());

        final FetchOutcome outcome = fetch.service().fetch("test", PageUrl.parse("https://example.com/"), Duration.ofMillis(100));

        assertInstanceOf(FetchOutcome.TimedOut.class, outcome);
    }

    /**
     * The box of the client holds its requests until their results are taken: a request that timed out still counts.
     */
    @Test
    void fullBoxRefusesTheFetchUntilAResultIsTaken() throws Exception {
        final CountDownLatch release = new CountDownLatch(1);
        fetch = new LocalFetch(attempt -> {
            if (LocalFetch.url(attempt).contains("a.com")) {
                release.await();
            }
            return Blob.of("page".getBytes(StandardCharsets.UTF_8));
        }, noBlobs(), 1);

        final Duration brief = Duration.ofMillis(100);
        assertInstanceOf(FetchOutcome.TimedOut.class, fetch.service().fetch("test", PageUrl.parse("https://a.com/"), brief));
        assertInstanceOf(FetchOutcome.Busy.class, fetch.service().fetch("test", PageUrl.parse("https://b.com/"), WAIT));
        assertInstanceOf(FetchOutcome.Fetched.class, fetch.service().fetch("other", PageUrl.parse("https://b.com/"), WAIT),
            "another client has a box of its own");

        release.countDown();
        final long deadline = System.nanoTime() + WAIT.toNanos();
        FetchOutcome outcome = fetch.service().fetch("test", PageUrl.parse("https://b.com/"), WAIT);
        while (outcome instanceof FetchOutcome.Busy && System.nanoTime() < deadline) {
            Thread.sleep(20);
            outcome = fetch.service().fetch("test", PageUrl.parse("https://b.com/"), WAIT);
        }
        assertInstanceOf(FetchOutcome.Fetched.class, outcome);
    }

    private static BlobReader noBlobs() {
        return ref -> {
            throw new IOException("no blobs in this test");
        };
    }
}
