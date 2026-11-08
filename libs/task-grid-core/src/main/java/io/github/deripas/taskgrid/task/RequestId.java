package io.github.deripas.taskgrid.task;

import java.util.Objects;

/**
 * Who asked for a task and under which id of its own: a client (an integration, e.g. a service that processes pages),
 * one of its consumers (an instance of it, stable across restarts - e.g. the ordinal of a pod) and the id the client
 * gave the request. The key of idempotency - a retried submission with the same id is the same request - and the
 * address of the result. Several requests with the same payload are one task of the queue, done once.
 */
public record RequestId(
    String client,
    String consumer,
    String id
) {
    /**
     * Separator of the client and the consumer in a string key; not allowed in the client.
     */
    public static final char SEPARATOR = '/';

    public RequestId {
        Objects.requireNonNull(client, "client");
        Objects.requireNonNull(consumer, "consumer");
        Objects.requireNonNull(id, "id");
        if (client.isBlank() || client.indexOf(SEPARATOR) >= 0) {
            throw new IllegalArgumentException("client must be non-blank and without '" + SEPARATOR + "': " + client);
        }
        if (consumer.isBlank()) {
            throw new IllegalArgumentException("consumer must be non-blank");
        }
        if (id.isBlank()) {
            throw new IllegalArgumentException("id must be non-blank");
        }
    }
}
