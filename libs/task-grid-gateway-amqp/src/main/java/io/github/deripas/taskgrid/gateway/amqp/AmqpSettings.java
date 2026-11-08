package io.github.deripas.taskgrid.gateway.amqp;

import java.time.Duration;
import java.util.Objects;

/**
 * Settings of the gateway over RabbitMQ.
 *
 * @param queue       of the request messages (declared durable if absent)
 * @param prefetch    request messages in work at once: not acknowledged yet
 * @param ack         when a request is acknowledged
 * @param timeout     a request of {@link Ack#AFTER_REPLY} waits this long for its result, then gets a refusal
 * @param contentType of the replies
 */
public record AmqpSettings(
    String queue,
    int prefetch,
    Ack ack,
    Duration timeout,
    String contentType
) {
    public AmqpSettings {
        Objects.requireNonNull(queue, "queue");
        Objects.requireNonNull(ack, "ack");
        Objects.requireNonNull(contentType, "contentType");
        if (prefetch < 1) {
            throw new IllegalArgumentException("prefetch must be >= 1: " + prefetch);
        }
        if (!timeout.isPositive()) {
            throw new IllegalArgumentException("timeout must be positive: " + timeout);
        }
    }
}
