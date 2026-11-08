package io.github.deripas.taskgrid.gateway.amqp;

import io.github.deripas.taskgrid.dispatch.Dispatcher;
import io.github.deripas.taskgrid.gateway.Gateway;
import io.github.deripas.taskgrid.gateway.InvalidPayloadException;
import io.github.deripas.taskgrid.gateway.TaskCodec;
import io.github.deripas.taskgrid.mailbox.MailboxSettings;
import io.github.deripas.taskgrid.mailbox.MailboxSink;
import io.github.deripas.taskgrid.mailbox.memory.MemoryMailbox;
import io.github.deripas.taskgrid.queue.DelayPolicy;
import io.github.deripas.taskgrid.queue.QueueSettings;
import io.github.deripas.taskgrid.queue.memory.MemoryTaskQueue;
import io.github.deripas.taskgrid.task.Blob;
import io.github.deripas.taskgrid.task.Task;
import io.github.deripas.taskgrid.task.TaskResult;
import io.github.deripas.taskgrid.task.TaskStatus;
import io.github.deripas.taskgrid.worker.Worker;
import io.github.deripas.taskgrid.worker.local.LocalWorker;
import io.github.deripas.taskgrid.worker.local.RetryableException;

import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertNotNull;

/**
 * The node and the broker side of the tests: an in-memory node with a handler of words, fake deliveries and replies.
 * Protocol of the tests: the payload is a word, a reply - {@code <id>:done:<word>}, {@code <id>:failed:<error>} or
 * {@code <id>:<REFUSAL>}; the word {@code down} fails, {@code hold} waits for {@link Node#release}, an empty one is
 * refused.
 */
final class Fakes {
    static final long WAIT_S = 5;

    private Fakes() {
    }

    static byte[] word(String word) {
        return word.getBytes(StandardCharsets.UTF_8);
    }

    static final class Node implements AutoCloseable {
        final ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor();
        final AtomicInteger calls = new AtomicInteger();
        final CountDownLatch release = new CountDownLatch(1);
        final MemoryTaskQueue queue = new MemoryTaskQueue(new QueueSettings(1, Duration.ofMinutes(1)), DelayPolicy.NONE);
        final MemoryMailbox mailbox = new MemoryMailbox(new MailboxSettings(10, Duration.ofMinutes(1)), Clock.systemUTC());
        final Gateway gateway = new Gateway(queue, mailbox, "node-1");
        final Words codec = new Words();

        Node() {
            queue.onResult(new MailboxSink(mailbox));
            Dispatcher.connect(queue, new LocalWorker(Map.of("words", attempt -> {
                calls.incrementAndGet();
                final String word = new String(((Blob.Inline) attempt.task().payload()).bytes(), StandardCharsets.UTF_8);
                if (word.equals("down")) {
                    throw new IllegalStateException("site unreachable");
                }
                if (word.equals("hold")) {
                    if (!release.await(WAIT_S, TimeUnit.SECONDS)) {
                        throw new RetryableException("not released");
                    }
                }
                return attempt.task().payload();
            }), executor));
        }

        @Override
        public void close() {
            executor.shutdownNow();
            queue.close();
        }
    }

    static final class Words implements TaskCodec {
        @Override
        public Task task(byte[] payload) throws InvalidPayloadException {
            if (payload.length == 0) {
                throw new InvalidPayloadException("empty word");
            }
            return Task.of(Blob.of(payload), Map.of(Worker.KIND, "words"));
        }

        @Override
        public byte[] reply(String id, TaskResult result) {
            return word(result.status() == TaskStatus.DONE
                ? id + ":done:" + new String(((Blob.Inline) result.body()).bytes(), StandardCharsets.UTF_8)
                : id + ":failed:" + result.error());
        }

        @Override
        public byte[] refusal(
            String id,
            Refusal refusal,
            String message
        ) {
            return word(id + ":" + refusal);
        }
    }

    /**
     * A delivered request: records the acknowledgement.
     */
    static final class FakeDelivery implements Delivery {
        final CountDownLatch acked = new CountDownLatch(1);
        final CountDownLatch requeued = new CountDownLatch(1);
        private final String replyTo;
        private final String correlationId;

        FakeDelivery(String replyTo, String correlationId) {
            this.replyTo = replyTo;
            this.correlationId = correlationId;
        }

        @Override
        public String replyTo() {
            return replyTo;
        }

        @Override
        public String correlationId() {
            return correlationId;
        }

        @Override
        public void ack() {
            acked.countDown();
        }

        @Override
        public void requeue() {
            requeued.countDown();
        }

        boolean acked() throws InterruptedException {
            return acked.await(WAIT_S, TimeUnit.SECONDS);
        }

        boolean requeued() throws InterruptedException {
            return requeued.await(WAIT_S, TimeUnit.SECONDS);
        }
    }

    /**
     * Records replies; {@link #confirm} - the answer of the broker to each of them.
     */
    static final class FakeReplies implements ReplyPublisher {
        final BlockingQueue<Sent> sent = new LinkedBlockingQueue<>();
        volatile CompletableFuture<Void> confirm = CompletableFuture.completedFuture(null);

        record Sent(
            String replyTo,
            String correlationId,
            String body
        ) {
        }

        @Override
        public CompletionStage<Void> publish(
            String replyTo,
            String correlationId,
            byte[] body
        ) {
            sent.add(new Sent(replyTo, correlationId, new String(body, StandardCharsets.UTF_8)));
            return confirm;
        }

        Sent next() throws InterruptedException {
            final Sent reply = sent.poll(WAIT_S, TimeUnit.SECONDS);
            assertNotNull(reply, "no reply");
            return reply;
        }

        Sent none() throws InterruptedException {
            return sent.poll(200, TimeUnit.MILLISECONDS);
        }
    }
}
