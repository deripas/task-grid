package io.github.deripas.crawler.urlmanager.amqp;

import com.rabbitmq.client.Channel;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.amqp.rabbit.listener.api.ChannelAwareMessageListener;

import java.io.IOException;

/**
 * Consumer of the request queue (manual acknowledgement): hands each message to {@link FetchRequests} and returns at
 * once; the acknowledgement comes later, from the thread of the request, on the channel of the delivery.
 */
@RequiredArgsConstructor
public final class RabbitFetchListener implements ChannelAwareMessageListener {
    public static final String CONTENT_TYPE = "application/x-protobuf";

    private final FetchRequests requests;

    @Override
    public void onMessage(Message message, Channel channel) {
        requests.handle(message.getBody(), new RabbitDelivery(message.getMessageProperties(), channel));
    }

    @Slf4j
    @RequiredArgsConstructor
    private static final class RabbitDelivery implements Delivery {
        private final MessageProperties properties;
        private final Channel channel;

        @Override
        public String replyTo() {
            return properties.getReplyTo();
        }

        @Override
        public String correlationId() {
            return properties.getCorrelationId();
        }

        @Override
        public String messageId() {
            return properties.getMessageId();
        }

        @Override
        public void ack() {
            try {
                channel.basicAck(properties.getDeliveryTag(), false);
            } catch (IOException | RuntimeException e) {
                // the channel is gone: the broker delivers the request again
                log.warn("Ack failed: delivery_tag={} error={}", properties.getDeliveryTag(), e.toString());
            }
        }

        @Override
        public void requeue() {
            try {
                channel.basicNack(properties.getDeliveryTag(), false, true);
            } catch (IOException | RuntimeException e) {
                log.warn("Nack failed: delivery_tag={} error={}", properties.getDeliveryTag(), e.toString());
            }
        }
    }
}
