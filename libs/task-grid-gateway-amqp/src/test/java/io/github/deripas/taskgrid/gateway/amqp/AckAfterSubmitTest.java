package io.github.deripas.taskgrid.gateway.amqp;

import io.github.deripas.taskgrid.gateway.amqp.Fakes.FakeDelivery;
import io.github.deripas.taskgrid.gateway.amqp.Fakes.FakeReplies;
import io.github.deripas.taskgrid.gateway.amqp.Fakes.Node;
import io.github.deripas.taskgrid.queue.ReadyCallback;
import io.github.deripas.taskgrid.queue.ResultCallback;
import io.github.deripas.taskgrid.queue.TaskQueue;
import io.github.deripas.taskgrid.task.AttemptResult;
import io.github.deripas.taskgrid.task.Request;
import io.github.deripas.taskgrid.task.RequestId;
import io.github.deripas.taskgrid.task.Task;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import static io.github.deripas.taskgrid.gateway.amqp.Fakes.word;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class AckAfterSubmitTest {
    private final Node node = new Node();
    private final FakeReplies replies = new FakeReplies();
    private final AckAfterSubmit requests = new AckAfterSubmit(node.queue, node.codec, replies, node.executor);

    AckAfterSubmitTest() {
        node.queue.onResult(requests);
    }

    @AfterEach
    void stop() {
        node.close();
    }

    @Test
    void requestIsAcknowledgedOnceQueuedAndTheResultRepliedToItsAddress() throws Exception {
        final FakeDelivery delivery = new FakeDelivery("client.replies", "c-1");
        requests.handle(word("hold"), delivery);

        assertTrue(delivery.acked());
        assertNull(replies.none(), "the result is not there yet");
        node.release.countDown();
        assertEquals(new FakeReplies.Sent("client.replies", "c-1", "c-1:done:hold"), replies.next());
    }

    /**
     * The reply goes to every request of the task: to the address of each.
     */
    @Test
    void requestsOfTheSamePayloadAreRepliedEach() throws Exception {
        requests.handle(word("hold"), new FakeDelivery("one.replies", "c-1"));
        requests.handle(word("hold"), new FakeDelivery("two.replies", "c-2"));
        Thread.sleep(200);
        node.release.countDown();

        final FakeReplies.Sent first = replies.next();
        final FakeReplies.Sent second = replies.next();
        assertEquals(Set.of("one.replies", "two.replies"), Set.of(first.replyTo(), second.replyTo()));
        assertEquals(1, node.calls.get());
    }

    @Test
    void requestTheQueueDoesNotTakeGoesBack() throws Exception {
        final FakeDelivery delivery = new FakeDelivery("client.replies", "c-1");
        final AckAfterSubmit failing = new AckAfterSubmit(new FailingQueue(), node.codec, replies, node.executor);
        failing.handle(word("page"), delivery);

        assertTrue(delivery.requeued());
    }

    @Test
    void resultsOfOtherWaysAreNotReplied() throws Exception {
        node.gateway.submit(new RequestId("pagenator", "0", "r1"), node.codec.task(word("page"))).toCompletableFuture().join();

        assertNull(replies.none());
    }

    @Test
    void invalidPayloadIsRefusedAndAcknowledgedAfterTheConfirm() throws Exception {
        replies.confirm = new CompletableFuture<>();
        final FakeDelivery delivery = new FakeDelivery("client.replies", "c-1");
        requests.handle(word(""), delivery);

        assertEquals("c-1:INVALID", replies.next().body());
        assertFalse(delivery.acked.getCount() == 0);
        replies.confirm.complete(null);
        assertTrue(delivery.acked());
    }

    @Test
    void requestWithoutReplyToIsAcknowledgedAndDropped() throws Exception {
        final FakeDelivery delivery = new FakeDelivery(" ", "c-1");
        requests.handle(word("page"), delivery);

        assertTrue(delivery.acked());
        assertEquals(0, node.calls.get());
        assertNull(replies.none());
    }

    /**
     * A queue that takes nothing.
     */
    private static final class FailingQueue implements TaskQueue {
        @Override
        public CompletionStage<Void> submit(Task task, Request request) {
            return CompletableFuture.failedFuture(new IllegalStateException("journal is down"));
        }

        @Override
        public CompletionStage<Void> complete(AttemptResult result) {
            return CompletableFuture.completedFuture(null);
        }

        @Override
        public void onReady(ReadyCallback callback) {
        }

        @Override
        public void onResult(ResultCallback callback) {
        }
    }
}
