package io.github.deripas.crawler.urlmanager.amqp;

/**
 * Handling of the request messages of the RabbitMQ queue: {@code crawler.urlmanager.v1.FetchRequest} in,
 * {@code FetchReply} to reply_to. Two ways, chosen by {@code url-manager.rabbit.ack}:
 * <ul>
 *   <li>{@link AckAfterSubmitRequests} - the target one: acknowledged once the task is queued, the reply is sent by
 *   the result callback of the queue, wherever the task finishes; needs a queue that keeps its tasks;</li>
 *   <li>{@link AckAfterReplyRequests} - for the in-memory queue, until it is gone: the request stays unacknowledged
 *   until its reply is confirmed, so a request of a url-manager that stopped is delivered again.</li>
 * </ul>
 * Common to both: a request that is not FetchRequest or has a bad URL is refused; one without reply_to is
 * acknowledged and dropped.
 */
public interface FetchRequests {
    /**
     * Returns at once: the request is handled on threads of the implementation.
     */
    void handle(byte[] body, Delivery delivery);
}
