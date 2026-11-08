package io.github.deripas.taskgrid.gateway;

/**
 * What became of a submitted request. A failure of the queue is a failed stage instead: the client retries.
 */
public sealed interface Submission {
    /**
     * In the queue, or its result waits in the box already.
     */
    record Accepted() implements Submission {
    }

    /**
     * The box of the consumer is full: its results must be acknowledged first.
     */
    record Busy() implements Submission {
    }

    /**
     * Refused for good: the payload is not accepted, or the id is in the queue with another payload.
     */
    record Rejected(String reason) implements Submission {
    }
}
