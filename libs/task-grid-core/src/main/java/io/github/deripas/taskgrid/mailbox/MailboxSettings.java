package io.github.deripas.taskgrid.mailbox;

import java.time.Duration;
import java.util.Objects;

/**
 * Settings of the mailboxes.
 *
 * @param limit requests of a box at once: submitted and not yet acknowledged by the consumer; a fuse against a client
 *              that submits without taking its results, not a way to balance the load
 * @param ttl   a request of a box unchanged this long is dropped: a reservation that never reached the queue,
 *              a result nobody takes; above the longest life of a task in the queue
 */
public record MailboxSettings(
    int limit,
    Duration ttl
) {
    public MailboxSettings {
        if (limit < 1) {
            throw new IllegalArgumentException("limit must be >= 1: " + limit);
        }
        Objects.requireNonNull(ttl, "ttl");
        if (!ttl.isPositive()) {
            throw new IllegalArgumentException("ttl must be positive: " + ttl);
        }
    }
}
