package io.github.deripas.crawler.urlmanager.amqp;

import lombok.RequiredArgsConstructor;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.amqp.rabbit.connection.CorrelationData;
import org.springframework.amqp.rabbit.core.RabbitTemplate;

import java.io.IOException;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

/**
 * Replies through the default exchange with publisher confirms ({@code spring.rabbitmq.publisher-confirm-type:
 * correlated}): the stage completes when the broker has taken the reply.
 */
@RequiredArgsConstructor
public final class RabbitReplyPublisher implements ReplyPublisher {
    private final RabbitTemplate template;

    @Override
    public CompletionStage<Void> publish(
        String replyTo,
        String correlationId,
        byte[] body
    ) {
        final MessageProperties properties = new MessageProperties();
        properties.setContentType(RabbitFetchListener.CONTENT_TYPE);
        properties.setCorrelationId(correlationId);
        final CorrelationData confirm = new CorrelationData(UUID.randomUUID().toString());
        try {
            template.send("", replyTo, new Message(body, properties), confirm);
        } catch (RuntimeException e) {
            return CompletableFuture.failedFuture(e);
        }
        return confirm.getFuture().thenCompose(c -> c.ack()
            ? CompletableFuture.<Void>completedFuture(null)
            : CompletableFuture.<Void>failedFuture(new IOException("reply nacked by the broker: " + c.reason())));
    }
}
