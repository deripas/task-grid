package io.github.deripas.taskgrid.gateway.amqp;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;

import java.io.IOException;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.TimeUnit;

/**
 * Replies through the default exchange with publisher confirms: the stage completes when the broker has taken the
 * reply, fails when it nacks it, the channel closes or no confirm comes in time. Opened by {@link AmqpGateway}; before
 * that a reply fails (the queue delivers its result again).
 */
final class ConfirmedPublisher implements ReplyPublisher {
    private static final Duration CONFIRM_TIMEOUT = Duration.ofSeconds(30);

    private final String contentType;
    /**
     * Publish sequence number -> the stage of the reply.
     */
    private final ConcurrentNavigableMap<Long, CompletableFuture<Void>> pending = new ConcurrentSkipListMap<>();
    private volatile Channel channel;

    ConfirmedPublisher(String contentType) {
        this.contentType = contentType;
    }

    void open(Connection connection) throws IOException {
        final Channel opened = connection.createChannel();
        opened.confirmSelect();
        opened.addConfirmListener((sequence, multiple) -> confirmed(sequence, multiple, null),
            (sequence, multiple) -> confirmed(sequence, multiple, new IOException("reply nacked by the broker")));
        opened.addShutdownListener(cause -> failAll(new IOException("reply channel closed: " + cause.getMessage())));
        channel = opened;
    }

    @Override
    public CompletionStage<Void> publish(
        String replyTo,
        String correlationId,
        byte[] body
    ) {
        final Channel current = channel;
        if (current == null) {
            return CompletableFuture.failedFuture(new IOException("the gateway is not started"));
        }
        final AMQP.BasicProperties properties = new AMQP.BasicProperties.Builder()
            .contentType(contentType)
            .correlationId(correlationId)
            .build();
        final CompletableFuture<Void> confirm = new CompletableFuture<>();
        synchronized (this) {
            final long sequence = current.getNextPublishSeqNo();
            pending.put(sequence, confirm);
            try {
                current.basicPublish("", replyTo, properties, body);
            } catch (IOException | RuntimeException e) {
                pending.remove(sequence);
                return CompletableFuture.failedFuture(e);
            }
        }
        return confirm.orTimeout(CONFIRM_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
    }

    private void confirmed(
        long sequence,
        boolean multiple,
        IOException error
    ) {
        final ConcurrentNavigableMap<Long, CompletableFuture<Void>> done = multiple
            ? pending.headMap(sequence, true)
            : pending.subMap(sequence, true, sequence, true);
        done.values().forEach(confirm -> {
            if (error == null) {
                confirm.complete(null);
            } else {
                confirm.completeExceptionally(error);
            }
        });
        done.clear();
    }

    /**
     * After a recovery the sequence numbers start again: what was not confirmed is failed.
     */
    private void failAll(IOException error) {
        pending.values().forEach(confirm -> confirm.completeExceptionally(error));
        pending.clear();
    }
}
