package io.github.deripas.crawler.urlmanager.amqp;

/**
 * One delivered request message: its reply address and its acknowledgement. Implemented over Spring AMQP by
 * {@link RabbitFetchListener}, by fakes in tests.
 */
interface Delivery {
    /**
     * {@code null} - no reply_to.
     */
    String replyTo();

    String correlationId();

    String messageId();

    /**
     * The request is handled: the broker forgets it.
     */
    void ack();

    /**
     * The request is not handled: the broker delivers it again.
     */
    void requeue();
}
