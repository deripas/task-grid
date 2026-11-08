package io.github.deripas.taskgrid.mailbox;

import io.github.deripas.taskgrid.task.RequestId;
import io.github.deripas.taskgrid.task.TaskResult;

import java.util.Collection;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Flow;

/**
 * The way back to a client whose task finishes on another node than the one it submitted to: results wait in the
 * {@link Box box} of the consumer until it acknowledges them; any node reaches any box.
 * <ul>
 *   <li>Before the submit, a request {@link #reserve reserves} a place in its box: a box holds at most
 *   {@link MailboxSettings#limit} requests submitted and not yet acknowledged.</li>
 *   <li>The final result of the task is {@link #put} into the box of each of its requests.</li>
 *   <li>The consumer {@link #subscribe subscribes} to its box and {@link #ack acknowledges} what it has processed:
 *   the result leaves the box and frees its place.</li>
 * </ul>
 * Delivery is at least once: a result not acknowledged comes again with the next subscription, a result put again
 * after its acknowledgement (the queue repeats a delivery that did not complete) comes again too. Requests unchanged
 * for {@link MailboxSettings#ttl} are dropped.
 */
public interface Mailbox {
    /**
     * Takes a place in the box for the request, or tells why it does not need one; idempotent by the request id.
     */
    CompletionStage<Reservation> reserve(RequestId request);

    /**
     * Frees the place of a {@link Reservation#NEW} request whose submit failed; a request with its result stays.
     */
    CompletionStage<Void> release(RequestId request);

    /**
     * The final result for the request: it waits for the consumer, also without a reservation (expired, never made)
     * and above the limit - the queue cannot take it back.
     */
    CompletionStage<Void> put(RequestId request, TaskResult result);

    /**
     * Replies of the box not yet acknowledged, in the order they were put, then the new ones, as the subscriber
     * requests them. One subscription per box: a newer one ends the older one with {@link SupersededException}.
     */
    void subscribe(Box box, Flow.Subscriber<? super Reply> subscriber);

    /**
     * Removes the replies to the requests from the box; unknown ids (acknowledged before, expired) are ignored.
     */
    CompletionStage<Void> ack(Box box, Collection<String> ids);
}
