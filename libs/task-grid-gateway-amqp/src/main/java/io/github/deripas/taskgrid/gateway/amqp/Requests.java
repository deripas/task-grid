package io.github.deripas.taskgrid.gateway.amqp;

import io.github.deripas.taskgrid.gateway.TaskCodec;
import io.github.deripas.taskgrid.task.Blob;
import lombok.extern.slf4j.Slf4j;

/**
 * A way of handling the request messages ({@link Ack}): {@link AckAfterReply}, {@link AckAfterSubmit}. Common to both:
 * the id of a request is the hash of its message - a message delivered again is the same request; a request without
 * reply_to is acknowledged and dropped; a payload the application does not accept is refused.
 */
@Slf4j
abstract class Requests {
    protected final TaskCodec codec;
    protected final ReplyPublisher replies;

    protected Requests(TaskCodec codec, ReplyPublisher replies) {
        this.codec = codec;
        this.replies = replies;
    }

    /**
     * Returns at once: the request is handled on threads of the implementation.
     */
    abstract void handle(byte[] body, Delivery delivery);

    static String id(byte[] body) {
        return Blob.of(body).hash();
    }

    static boolean hasReplyTo(Delivery delivery) {
        if (delivery.replyTo() != null && !delivery.replyTo().isBlank()) {
            return true;
        }
        log.warn("Request without reply_to dropped: correlation_id={}", delivery.correlationId());
        return false;
    }

    /**
     * The id of the request in its replies: its correlation id, else the hash of the message.
     */
    static String replyId(Delivery delivery, String id) {
        return delivery.correlationId() != null
            ? delivery.correlationId()
            : id;
    }

    /**
     * Acknowledges the request when the broker has confirmed the reply, otherwise puts it back.
     */
    protected void send(Delivery delivery, byte[] reply) {
        replies.publish(delivery.replyTo(), delivery.correlationId(), reply).whenComplete((ignored, error) -> {
            if (error == null) {
                delivery.ack();
            } else {
                log.warn("Reply not confirmed, the request goes back to the queue: correlation_id={} error={}",
                    delivery.correlationId(), error.toString());
                delivery.requeue();
            }
        });
    }
}
