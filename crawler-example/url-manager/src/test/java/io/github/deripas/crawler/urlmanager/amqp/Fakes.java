package io.github.deripas.crawler.urlmanager.amqp;

import com.google.protobuf.InvalidProtocolBufferException;
import io.github.deripas.crawler.urlmanager.fetch.LocalFetch;
import io.github.deripas.crawler.urlmanager.v1.FetchReply;
import io.github.deripas.crawler.urlmanager.v1.FetchRequest;
import io.github.deripas.taskgrid.task.Blob;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertNotNull;

/**
 * The broker side of the tests of both ways of handling, and the fetch behind them.
 */
final class Fakes {
    static final long WAIT_S = 5;

    private Fakes() {
    }

    static byte[] request(String id, String url) {
        return FetchRequest.newBuilder().setRequestId(id).setUrl(url).build().toByteArray();
    }

    /**
     * A fetch with a handler in the JVM: "page of <url>"; a URL ending with /down fails, one ending with /hold waits
     * for {@link #release}.
     */
    static final class Site {
        final AtomicInteger calls = new AtomicInteger();
        final CountDownLatch release = new CountDownLatch(1);
        final LocalFetch fetch = new LocalFetch(attempt -> {
            calls.incrementAndGet();
            final String url = LocalFetch.url(attempt);
            if (url.endsWith("/down")) {
                throw new IllegalStateException("FailedPrecondition: site unreachable");
            }
            if (url.endsWith("/hold")) {
                release.await(WAIT_S, TimeUnit.SECONDS);
            }
            return Blob.of(("page of " + url).getBytes(StandardCharsets.UTF_8));
        }, ref -> {
            throw new IOException("no blobs");
        });
    }

    /**
     * A delivered request: records the acknowledgement.
     */
    static final class FakeDelivery implements Delivery {
        final CountDownLatch acked = new CountDownLatch(1);
        final CountDownLatch requeued = new CountDownLatch(1);
        private final String replyTo;
        private final String messageId;

        FakeDelivery(String replyTo, String messageId) {
            this.replyTo = replyTo;
            this.messageId = messageId;
        }

        @Override
        public String replyTo() {
            return replyTo;
        }

        @Override
        public String correlationId() {
            return "c-" + messageId;
        }

        @Override
        public String messageId() {
            return messageId;
        }

        @Override
        public void ack() {
            acked.countDown();
        }

        @Override
        public void requeue() {
            requeued.countDown();
        }

        boolean isAcked() {
            return acked.getCount() == 0;
        }

        boolean isRequeued() {
            return requeued.getCount() == 0;
        }
    }

    /**
     * Records replies; {@link #confirm} - the answer of the broker to each of them.
     */
    static final class FakeReplies implements ReplyPublisher {
        final BlockingQueue<Sent> sent = new LinkedBlockingQueue<>();
        volatile CompletableFuture<Void> confirm = CompletableFuture.completedFuture(null);

        record Sent(String replyTo, String correlationId, FetchReply reply) {
        }

        @Override
        public CompletionStage<Void> publish(
            String replyTo,
            String correlationId,
            byte[] body
        ) {
            try {
                sent.add(new Sent(replyTo, correlationId, FetchReply.parseFrom(body)));
            } catch (InvalidProtocolBufferException e) {
                throw new IllegalStateException(e);
            }
            return confirm;
        }

        Sent next() throws InterruptedException {
            final Sent reply = sent.poll(WAIT_S, TimeUnit.SECONDS);
            assertNotNull(reply, "no reply");
            return reply;
        }

        Sent none() throws InterruptedException {
            return sent.poll(100, TimeUnit.MILLISECONDS);
        }
    }
}
