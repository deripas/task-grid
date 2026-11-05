package io.github.deripas.taskgrid.queue;

import java.time.Duration;
import java.util.Objects;

/**
 * Settings of a task queue.
 *
 * @param maxAttempts    attempts per task, including the first one; exhausted - the task fails
 * @param attemptTimeout an attempt without a result this long counts as a technical failure (RETRY): the limit for
 *                       a hung or lost attempt (its worker is gone with a node), not a deadline of the work
 */
public record QueueSettings(
    int maxAttempts,
    Duration attemptTimeout
) {
    public QueueSettings {
        if (maxAttempts < 1) {
            throw new IllegalArgumentException("maxAttempts must be >= 1: " + maxAttempts);
        }
        Objects.requireNonNull(attemptTimeout, "attemptTimeout");
        if (!attemptTimeout.isPositive()) {
            throw new IllegalArgumentException("attemptTimeout must be positive: " + attemptTimeout);
        }
    }
}
