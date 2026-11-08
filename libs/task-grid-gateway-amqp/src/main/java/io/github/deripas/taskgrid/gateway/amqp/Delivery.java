package io.github.deripas.taskgrid.gateway.amqp;

/**
 * One delivered request message: its reply address and its acknowledgement. Over a channel of the broker
 * ({@link AmqpGateway}), fakes in tests.
 */
interface Delivery {
    /**
     * {@code null} - no reply_to.
     */
    String replyTo();

    /**
     * {@code null} - none.
     */
    String correlationId();

    /**
     * The request is handled: the broker forgets it.
     */
    void ack();

    /**
     * The request is not handled: the broker delivers it again.
     */
    void requeue();
}
