package io.github.deripas.crawler.urlmanager.amqp;

import java.util.concurrent.CompletionStage;

/**
 * Publishes a reply to a reply_to queue; completes when the broker has confirmed it. Implemented over Spring AMQP by
 * {@link RabbitReplyPublisher}, by fakes in tests.
 */
interface ReplyPublisher {
    CompletionStage<Void> publish(
        String replyTo,
        String correlationId,
        byte[] body
    );
}
