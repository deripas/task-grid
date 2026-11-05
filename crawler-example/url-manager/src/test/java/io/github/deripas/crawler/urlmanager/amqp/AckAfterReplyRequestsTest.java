package io.github.deripas.crawler.urlmanager.amqp;

import io.github.deripas.crawler.urlmanager.amqp.Fakes.FakeDelivery;
import io.github.deripas.crawler.urlmanager.amqp.Fakes.FakeReplies;
import io.github.deripas.crawler.urlmanager.amqp.Fakes.Site;
import io.github.deripas.crawler.urlmanager.v1.FetchReply;
import io.github.deripas.crawler.urlmanager.v1.FetchRequest;
import io.github.deripas.crawler.urlmanager.v1.Refusal;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static io.github.deripas.crawler.urlmanager.amqp.Fakes.WAIT_S;
import static io.github.deripas.crawler.urlmanager.amqp.Fakes.request;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The transitional way: the request is acknowledged after its reply is confirmed.
 */
class AckAfterReplyRequestsTest {
    private final ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor();
    private final Site site = new Site();
    private final FakeReplies replies = new FakeReplies();
    private final AckAfterReplyRequests requests = new AckAfterReplyRequests(site.fetch.service(), replies, executor,
        Duration.ofSeconds(WAIT_S), Duration.ofSeconds(WAIT_S));

    @AfterEach
    void stop() {
        site.fetch.close();
        executor.shutdownNow();
    }

    @Test
    void pageIsRepliedAndTheRequestAcknowledgedAfterTheConfirm() throws Exception {
        replies.confirm = new CompletableFuture<>();
        final FakeDelivery delivery = new FakeDelivery("reply-queue", "m-1");
        requests.handle(request("r-1", "https://example.com/a"), delivery);

        final FakeReplies.Sent sent = replies.next();
        assertEquals("reply-queue", sent.replyTo());
        assertEquals("c-m-1", sent.correlationId());
        assertEquals("r-1", sent.reply().getRequestId());
        assertEquals("page of https://example.com/a", sent.reply().getPage().getBody().toStringUtf8());
        assertFalse(delivery.acked.await(100, TimeUnit.MILLISECONDS), "acknowledged before the confirm");

        replies.confirm.complete(null);
        assertTrue(delivery.acked.await(WAIT_S, TimeUnit.SECONDS));
        assertFalse(delivery.isRequeued());
    }

    @Test
    void unconfirmedReplyPutsTheRequestBack() throws Exception {
        replies.confirm = CompletableFuture.failedFuture(new IOException("nacked"));
        final FakeDelivery delivery = new FakeDelivery("reply-queue", "m-1");
        requests.handle(request("r-1", "https://example.com/a"), delivery);

        assertTrue(delivery.requeued.await(WAIT_S, TimeUnit.SECONDS));
        assertFalse(delivery.isAcked());
    }

    @Test
    void requestDeliveredAgainWhileQueuedIsFetchedOnce() throws Exception {
        final FakeDelivery first = new FakeDelivery("reply-queue", "m-1");
        final FakeDelivery again = new FakeDelivery("reply-queue", "m-1");
        requests.handle(request("r-1", "https://example.com/hold"), first);
        requests.handle(request("r-1", "https://example.com/hold"), again);
        Thread.sleep(100);
        site.release.countDown();

        assertEquals("r-1", replies.next().reply().getRequestId());
        assertEquals("r-1", replies.next().reply().getRequestId());
        assertEquals(1, site.calls.get());
    }

    @Test
    void requestIdIsTheMessageIdWhenAbsent() throws Exception {
        requests.handle(request("", "https://example.com/a"), new FakeDelivery("reply-queue", "m-7"));

        assertEquals("m-7", replies.next().reply().getRequestId());
    }

    @Test
    void failedFetchIsRefused() throws Exception {
        final FakeDelivery delivery = new FakeDelivery("reply-queue", "m-1");
        requests.handle(request("r-1", "https://example.com/down"), delivery);

        final Refusal refusal = replies.next().reply().getRefusal();
        assertEquals(Refusal.Code.CODE_FAILED, refusal.getCode());
        assertTrue(refusal.getMessage().contains("site unreachable"), refusal.getMessage());
        assertTrue(delivery.acked.await(WAIT_S, TimeUnit.SECONDS));
    }

    @Test
    void noPageWithinTheTimeoutIsRefused() throws Exception {
        final FakeDelivery delivery = new FakeDelivery("reply-queue", "m-1");
        final byte[] body = FetchRequest.newBuilder()
            .setRequestId("r-1")
            .setUrl("https://example.com/hold")
            .setTimeoutMs(100)
            .build()
            .toByteArray();
        requests.handle(body, delivery);

        assertEquals(Refusal.Code.CODE_TIMEOUT, replies.next().reply().getRefusal().getCode());
        assertTrue(delivery.acked.await(WAIT_S, TimeUnit.SECONDS));
    }

    @Test
    void badUrlIsRefusedAndAcknowledged() throws Exception {
        final FakeDelivery delivery = new FakeDelivery("reply-queue", "m-1");
        requests.handle(request("r-1", "ftp://example.com/a"), delivery);

        assertEquals(Refusal.Code.CODE_INVALID, replies.next().reply().getRefusal().getCode());
        assertTrue(delivery.acked.await(WAIT_S, TimeUnit.SECONDS));
        assertEquals(0, site.calls.get());
    }

    @Test
    void bodyThatIsNotARequestIsRefused() throws Exception {
        final FakeDelivery delivery = new FakeDelivery("reply-queue", "m-1");
        requests.handle(new byte[] {(byte) 0xff, 0x01}, delivery);

        final FetchReply reply = replies.next().reply();
        assertEquals(Refusal.Code.CODE_INVALID, reply.getRefusal().getCode());
        assertTrue(delivery.acked.await(WAIT_S, TimeUnit.SECONDS));
    }

    @Test
    void requestWithoutReplyToIsAcknowledgedAndDropped() throws Exception {
        final FakeDelivery delivery = new FakeDelivery(null, "m-1");
        requests.handle(request("r-1", "https://example.com/a"), delivery);

        assertTrue(delivery.acked.await(WAIT_S, TimeUnit.SECONDS));
        assertNull(replies.none());
        assertEquals(0, site.calls.get());
    }
}
