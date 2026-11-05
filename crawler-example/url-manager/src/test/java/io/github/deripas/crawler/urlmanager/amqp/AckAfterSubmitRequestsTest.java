package io.github.deripas.crawler.urlmanager.amqp;

import io.github.deripas.crawler.urlmanager.amqp.Fakes.FakeDelivery;
import io.github.deripas.crawler.urlmanager.amqp.Fakes.FakeReplies;
import io.github.deripas.crawler.urlmanager.amqp.Fakes.Site;
import io.github.deripas.crawler.urlmanager.fetch.FetchOutcome;
import io.github.deripas.crawler.urlmanager.fetch.LocalFetch;
import io.github.deripas.crawler.urlmanager.fetch.PageUrl;
import io.github.deripas.crawler.urlmanager.v1.Refusal;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static io.github.deripas.crawler.urlmanager.amqp.Fakes.WAIT_S;
import static io.github.deripas.crawler.urlmanager.amqp.Fakes.request;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The target way: acknowledged once queued, the reply comes from the result callback of the queue.
 */
class AckAfterSubmitRequestsTest {
    private final ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor();
    private final Site site = new Site();
    private final FakeReplies replies = new FakeReplies();
    private final AckAfterSubmitRequests requests = new AckAfterSubmitRequests(site.fetch.service(), replies, executor);

    @BeforeEach
    void register() {
        site.fetch.service().onFinished(requests);
    }

    @AfterEach
    void stop() {
        site.fetch.close();
        executor.shutdownNow();
    }

    @Test
    void requestIsAcknowledgedOnceQueuedAndTheResultRepliedToItsAddress() throws Exception {
        final FakeDelivery delivery = new FakeDelivery("reply-queue", "m-1");
        requests.handle(request("r-1", "https://example.com/hold"), delivery);

        assertTrue(delivery.acked.await(WAIT_S, TimeUnit.SECONDS), "not acknowledged while the page is fetched");
        assertNull(replies.none());

        site.release.countDown();
        final FakeReplies.Sent sent = replies.next();
        assertEquals("reply-queue", sent.replyTo());
        assertEquals("c-m-1", sent.correlationId());
        assertEquals("r-1", sent.reply().getRequestId());
        assertEquals("page of https://example.com/hold", sent.reply().getPage().getBody().toStringUtf8());
    }

    @Test
    void failedFetchIsRefusedByTheCallback() throws Exception {
        requests.handle(request("r-1", "https://example.com/down"), new FakeDelivery("reply-queue", "m-1"));

        final Refusal refusal = replies.next().reply().getRefusal();
        assertEquals(Refusal.Code.CODE_FAILED, refusal.getCode());
        assertTrue(refusal.getMessage().contains("site unreachable"), refusal.getMessage());
    }

    /**
     * The queue refuses another URL under an id that is still queued: the request goes back to the broker.
     */
    @Test
    void requestTheQueueDoesNotTakeGoesBack() throws Exception {
        final FakeDelivery first = new FakeDelivery("reply-queue", "m-1");
        final FakeDelivery clash = new FakeDelivery("reply-queue", "m-2");
        requests.handle(request("r-1", "https://example.com/hold"), first);
        assertTrue(first.acked.await(WAIT_S, TimeUnit.SECONDS));

        requests.handle(request("r-1", "https://example.com/other"), clash);

        assertTrue(clash.requeued.await(WAIT_S, TimeUnit.SECONDS));
        assertFalse(clash.isAcked());
    }

    /**
     * Fetches submitted another way (POST /fetch) carry no reply address: the callback leaves them alone.
     */
    @Test
    void fetchesWithoutAReplyAddressAreNotReplied() throws Exception {
        final FetchOutcome outcome = site.fetch.service().fetch(PageUrl.parse("https://example.com/a"), Duration.ofSeconds(WAIT_S));

        assertEquals("page of https://example.com/a", LocalFetch.text(assertInstanceOf(FetchOutcome.Fetched.class, outcome).page()));
        assertNull(replies.none());
    }

    @Test
    void badUrlIsRefusedAndAcknowledgedAfterTheConfirm() throws Exception {
        final FakeDelivery delivery = new FakeDelivery("reply-queue", "m-1");
        requests.handle(request("r-1", "ftp://example.com/a"), delivery);

        assertEquals(Refusal.Code.CODE_INVALID, replies.next().reply().getRefusal().getCode());
        assertTrue(delivery.acked.await(WAIT_S, TimeUnit.SECONDS));
        assertEquals(0, site.calls.get());
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
