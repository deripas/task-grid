package io.github.deripas.taskgrid.gateway;

/**
 * A payload the application does not accept: the request is refused, not retried.
 */
public final class InvalidPayloadException extends Exception {
    public InvalidPayloadException(String message) {
        super(message);
    }
}
