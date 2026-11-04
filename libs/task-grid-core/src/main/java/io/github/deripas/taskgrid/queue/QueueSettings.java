package io.github.deripas.taskgrid.queue;

/**
 * Settings of a task queue.
 *
 * @param maxAttempts attempts per task, including the first one; exhausted - the task fails
 */
public record QueueSettings(int maxAttempts) {
    public QueueSettings {
        if (maxAttempts < 1) {
            throw new IllegalArgumentException("maxAttempts must be >= 1: " + maxAttempts);
        }
    }
}
