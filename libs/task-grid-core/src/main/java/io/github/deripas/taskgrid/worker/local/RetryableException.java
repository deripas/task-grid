package io.github.deripas.taskgrid.worker.local;

/**
 * Thrown by a {@link Handler} when another attempt may succeed (connection reset, timeout, overload):
 * the attempt counts and the task is retried while attempts remain. Any other exception fails the task at once.
 */
public class RetryableException extends Exception {
    public RetryableException(String message) {
        super(message);
    }

    public RetryableException(String message, Throwable cause) {
        super(message, cause);
    }
}
