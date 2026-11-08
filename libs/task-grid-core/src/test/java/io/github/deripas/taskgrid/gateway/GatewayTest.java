package io.github.deripas.taskgrid.gateway;

import io.github.deripas.taskgrid.dispatch.Dispatcher;
import io.github.deripas.taskgrid.mailbox.MailboxSettings;
import io.github.deripas.taskgrid.mailbox.MailboxSink;
import io.github.deripas.taskgrid.mailbox.memory.MemoryMailbox;
import io.github.deripas.taskgrid.queue.DelayPolicy;
import io.github.deripas.taskgrid.queue.QueueSettings;
import io.github.deripas.taskgrid.queue.memory.MemoryTaskQueue;
import io.github.deripas.taskgrid.task.Blob;
import io.github.deripas.taskgrid.task.RequestId;
import io.github.deripas.taskgrid.task.Task;
import io.github.deripas.taskgrid.worker.Worker;
import io.github.deripas.taskgrid.worker.local.LocalWorker;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;

/**
 * The gateway on an in-memory node with an in-process handler that echoes the payload.
 */
class GatewayTest {
    private static final Duration WAIT = Duration.ofSeconds(5);

    private final ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor();
    private final AtomicInteger handled = new AtomicInteger();
    private final MemoryTaskQueue queue = new MemoryTaskQueue(new QueueSettings(3, Duration.ofMinutes(1)), DelayPolicy.NONE);
    private final MemoryMailbox mailbox = new MemoryMailbox(new MailboxSettings(1, Duration.ofMinutes(1)), Clock.systemUTC());
    private final Gateway gateway = new Gateway(queue, mailbox, "node-1");
    /**
     * The task of the word {@code a} waits for it.
     */
    private volatile CompletableFuture<Void> gate = CompletableFuture.completedFuture(null);

    GatewayTest() {
        queue.onResult(new MailboxSink(mailbox));
        Dispatcher.connect(queue, new LocalWorker(Map.of("echo", attempt -> {
            handled.incrementAndGet();
            if ("a".equals(new String(((Blob.Inline) attempt.task().payload()).bytes(), StandardCharsets.UTF_8))) {
                gate.join();
            }
            return attempt.task().payload();
        }), executor));
    }

    @AfterEach
    void stop() {
        executor.shutdownNow();
        queue.close();
    }

    @Test
    void callWaitsForTheResultOnThisNode() {
        final Call call = join(gateway.call("rest", "a", task("a"), WAIT));

        assertEquals("a", text(assertInstanceOf(Call.Done.class, call)));
    }

    @Test
    void callsWithTheSameIdWaitForOneRequest() {
        gate = new CompletableFuture<>();
        final CompletionStage<Call> first = gateway.call("rest", "a", task("a"), WAIT);
        final CompletionStage<Call> second = gateway.call("rest", "a", task("a"), WAIT);
        gate.complete(null);

        assertEquals("a", text(assertInstanceOf(Call.Done.class, join(first))));
        assertEquals("a", text(assertInstanceOf(Call.Done.class, join(second))));
        assertEquals(1, handled.get());
    }

    /**
     * The box of the client holds its requests until their results are taken: one that timed out still counts.
     */
    @Test
    void timedOutRequestKeepsItsPlaceUntilItsResultComes() throws InterruptedException {
        gate = new CompletableFuture<>();
        assertInstanceOf(Call.TimedOut.class, join(gateway.call("rest", "a", task("a"), Duration.ofMillis(100))));
        assertInstanceOf(Call.Busy.class, join(gateway.call("rest", "b", task("b"), WAIT)));
        assertInstanceOf(Call.Done.class, join(gateway.call("amqp", "b", task("b"), WAIT)), "another client, another box");

        gate.complete(null);
        Call call = join(gateway.call("rest", "b", task("b"), WAIT));
        final long deadline = System.nanoTime() + WAIT.toNanos();
        while (call instanceof Call.Busy && System.nanoTime() < deadline) {
            Thread.sleep(20);
            call = join(gateway.call("rest", "b", task("b"), WAIT));
        }
        assertInstanceOf(Call.Done.class, call);
    }

    @Test
    void submitOfAFullBoxIsBusy() {
        assertInstanceOf(Submission.Accepted.class, join(gateway.submit(new RequestId("pagenator", "0", "a"), task("a"))));
        assertInstanceOf(Submission.Accepted.class, join(gateway.submit(new RequestId("pagenator", "0", "a"), task("a"))),
            "the same request again");
        assertInstanceOf(Submission.Busy.class, join(gateway.submit(new RequestId("pagenator", "0", "b"), task("b"))),
            "the result of a is not acknowledged");
    }

    private static Task task(String word) {
        return Task.of(Blob.of(word.getBytes(StandardCharsets.UTF_8)), Map.of(Worker.KIND, "echo"));
    }

    private static String text(Call.Done done) {
        return new String(((Blob.Inline) done.result().body()).bytes(), StandardCharsets.UTF_8);
    }

    private static <T> T join(CompletionStage<T> stage) {
        return stage.toCompletableFuture().orTimeout(WAIT.toSeconds() * 2, TimeUnit.SECONDS).join();
    }
}
