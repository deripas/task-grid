package io.github.deripas.taskgrid.gateway.amqp;

import io.github.deripas.taskgrid.gateway.amqp.Fakes.FakeDelivery;
import io.github.deripas.taskgrid.gateway.amqp.Fakes.FakeReplies;
import io.github.deripas.taskgrid.gateway.amqp.Fakes.Node;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;

import static io.github.deripas.taskgrid.gateway.amqp.Fakes.word;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class AckAfterReplyTest {
    private final Node node = new Node();
    private final FakeReplies replies = new FakeReplies();
    private final AckAfterReply requests = new AckAfterReply(node.gateway, node.codec, replies, node.executor, Duration.ofSeconds(2));

    @AfterEach
    void stop() {
        node.close();
    }

    @Test
    void resultIsRepliedAndTheRequestAcknowledgedAfterTheConfirm() throws Exception {
        replies.confirm = new CompletableFuture<>();
        final FakeDelivery delivery = new FakeDelivery("client.replies", "c-1");
        requests.handle(word("page"), delivery);

        assertEquals(new FakeReplies.Sent("client.replies", "c-1", "c-1:done:page"), replies.next());
        assertFalse(delivery.acked.getCount() == 0, "not before the confirm");
        replies.confirm.complete(null);
        assertTrue(delivery.acked());
    }

    @Test
    void unconfirmedReplyPutsTheRequestBack() throws Exception {
        replies.confirm = CompletableFuture.failedFuture(new IllegalStateException("broker is down"));
        final FakeDelivery delivery = new FakeDelivery("client.replies", "c-1");
        requests.handle(word("page"), delivery);

        assertTrue(delivery.requeued());
    }

    /**
     * The id of a request is the hash of its message: delivered again while it waits, it is the same request.
     */
    @Test
    void requestDeliveredAgainIsDoneOnce() throws Exception {
        final FakeDelivery first = new FakeDelivery("client.replies", "c-1");
        final FakeDelivery again = new FakeDelivery("client.replies", "c-1");
        requests.handle(word("hold"), first);
        requests.handle(word("hold"), again);
        Thread.sleep(200);
        node.release.countDown();

        assertTrue(first.acked());
        assertTrue(again.acked());
        assertEquals(1, node.calls.get());
    }

    @Test
    void failedTaskIsRepliedByTheApplication() throws Exception {
        requests.handle(word("down"), new FakeDelivery("client.replies", "c-1"));

        assertTrue(replies.next().body().startsWith("c-1:failed:"));
    }

    @Test
    void noResultWithinTheTimeoutIsRefused() throws Exception {
        requests.handle(word("hold"), new FakeDelivery("client.replies", "c-1"));

        assertEquals("c-1:TIMEOUT", replies.next().body());
        node.release.countDown();
    }

    @Test
    void invalidPayloadIsRefusedAndAcknowledged() throws Exception {
        final FakeDelivery delivery = new FakeDelivery("client.replies", "c-1");
        requests.handle(word(""), delivery);

        assertEquals("c-1:INVALID", replies.next().body());
        assertTrue(delivery.acked());
    }

    @Test
    void requestWithoutReplyToIsAcknowledgedAndDropped() throws Exception {
        final FakeDelivery delivery = new FakeDelivery(null, "c-1");
        requests.handle(word("page"), delivery);

        assertTrue(delivery.acked());
        assertNull(replies.none());
        assertEquals(0, node.calls.get());
    }
}
