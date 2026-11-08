package io.github.deripas.taskgrid.gateway.amqp;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;
import io.github.deripas.taskgrid.gateway.Gateway;
import io.github.deripas.taskgrid.gateway.TaskCodec;
import io.github.deripas.taskgrid.queue.TaskQueue;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeoutException;

/**
 * Clients over RabbitMQ: request messages of a queue (manual acknowledgement, prefetch), replies to their reply_to
 * with the same correlation id, confirmed by the broker. The payload of a request and the body of a reply are those of
 * the {@link TaskCodec} of the application; the way of handling is {@link AmqpSettings#ack()}.
 * <p>
 * Create it with the node, before the queue takes tasks (after-submit registers its result callback), then
 * {@link #start} and {@link #close}. The connection factory is the application's; automatic recovery is turned on:
 * the consumer comes back after a lost connection, the requests in work then are delivered again by the broker.
 */
@Slf4j
public final class AmqpGateway implements AutoCloseable {
    private final ConnectionFactory factory;
    private final AmqpSettings settings;
    private final ConfirmedPublisher publisher;
    private final Requests requests;
    private Connection connection;

    /**
     * @param gateway  the requests of {@link Ack#AFTER_REPLY} wait for their results through it
     * @param executor the requests are handled and the replies made on it: virtual threads
     */
    public AmqpGateway(
        ConnectionFactory factory,
        AmqpSettings settings,
        TaskQueue queue,
        Gateway gateway,
        TaskCodec codec,
        Executor executor
    ) {
        this.factory = factory;
        this.settings = settings;
        this.publisher = new ConfirmedPublisher(settings.contentType());
        this.requests = switch (settings.ack()) {
            case AFTER_REPLY -> new AckAfterReply(gateway, codec, publisher, executor, settings.timeout());
            case AFTER_SUBMIT -> {
                final AckAfterSubmit afterSubmit = new AckAfterSubmit(queue, codec, publisher, executor);
                queue.onResult(afterSubmit);
                yield afterSubmit;
            }
        };
    }

    /**
     * Connects, declares the request queue (durable) if absent and starts consuming.
     */
    public synchronized void start() throws IOException, TimeoutException {
        factory.setAutomaticRecoveryEnabled(true);
        connection = factory.newConnection("task-grid-gateway-amqp");
        publisher.open(connection);
        final Channel consumer = connection.createChannel();
        consumer.basicQos(settings.prefetch());
        consumer.queueDeclare(settings.queue(), true, false, false, null);
        consumer.basicConsume(settings.queue(), false, new Consumer(consumer, requests));
        log.info("Consuming requests: queue={} ack={} prefetch={}", settings.queue(), settings.ack(), settings.prefetch());
    }

    /**
     * Closes the connection: requests not acknowledged go back to the queue.
     */
    @Override
    public synchronized void close() {
        if (connection == null) {
            return;
        }
        try {
            connection.close();
        } catch (IOException | RuntimeException e) {
            log.warn("Connection not closed cleanly: {}", e.toString());
        }
        connection = null;
    }

    /**
     * Hands each message over and returns at once: acknowledged later, from the thread of the request.
     */
    private static final class Consumer extends DefaultConsumer {
        private final Requests requests;

        private Consumer(Channel channel, Requests requests) {
            super(channel);
            this.requests = requests;
        }

        @Override
        public void handleDelivery(
            String consumerTag,
            Envelope envelope,
            AMQP.BasicProperties properties,
            byte[] body
        ) {
            requests.handle(body, new ChannelDelivery(getChannel(), envelope.getDeliveryTag(), properties));
        }
    }

    @Slf4j
    @RequiredArgsConstructor
    private static final class ChannelDelivery implements Delivery {
        private final Channel channel;
        private final long tag;
        private final AMQP.BasicProperties properties;

        @Override
        public String replyTo() {
            return properties.getReplyTo();
        }

        @Override
        public String correlationId() {
            return properties.getCorrelationId();
        }

        @Override
        public void ack() {
            try {
                channel.basicAck(tag, false);
            } catch (IOException | RuntimeException e) {
                // the channel is gone: the broker delivers the request again
                log.warn("Ack failed: delivery_tag={} error={}", tag, e.toString());
            }
        }

        @Override
        public void requeue() {
            try {
                channel.basicNack(tag, false, true);
            } catch (IOException | RuntimeException e) {
                log.warn("Nack failed: delivery_tag={} error={}", tag, e.toString());
            }
        }
    }
}
