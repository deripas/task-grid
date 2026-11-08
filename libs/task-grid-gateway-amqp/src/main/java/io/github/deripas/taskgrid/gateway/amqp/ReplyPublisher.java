package io.github.deripas.taskgrid.gateway.amqp;

import java.util.concurrent.CompletionStage;

/**
 * Publishes a reply to a reply_to queue; completes when the broker has confirmed it. {@link ConfirmedPublisher},
 * fakes in tests.
 */
interface ReplyPublisher {
    CompletionStage<Void> publish(
        String replyTo,
        String correlationId,
        byte[] body
    );
}
