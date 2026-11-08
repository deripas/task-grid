package io.github.deripas.taskgrid.mailbox;

import io.github.deripas.taskgrid.task.RequestId;

import java.util.Objects;

/**
 * Address of a mailbox: a client and one of its consumers ({@link RequestId} without the id). The consumer must
 * survive a restart of its instance, otherwise the results waiting for it are taken by nobody until they expire.
 */
public record Box(
    String client,
    String consumer
) {
    public Box {
        Objects.requireNonNull(client, "client");
        Objects.requireNonNull(consumer, "consumer");
        if (client.isBlank() || client.indexOf(RequestId.SEPARATOR) >= 0) {
            throw new IllegalArgumentException("client must be non-blank and without '" + RequestId.SEPARATOR + "': " + client);
        }
        if (consumer.isBlank()) {
            throw new IllegalArgumentException("consumer must be non-blank");
        }
    }

    public static Box of(RequestId request) {
        return new Box(request.client(), request.consumer());
    }

    public RequestId request(String id) {
        return new RequestId(client, consumer, id);
    }

    /**
     * The address as one string, {@code client/consumer}; see {@link #parse}.
     */
    public String key() {
        return client + RequestId.SEPARATOR + consumer;
    }

    public static Box parse(String key) {
        final int at = key.indexOf(RequestId.SEPARATOR);
        if (at < 0) {
            throw new IllegalArgumentException("not a box key: " + key);
        }
        return new Box(key.substring(0, at), key.substring(at + 1));
    }

    @Override
    public String toString() {
        return key();
    }
}
