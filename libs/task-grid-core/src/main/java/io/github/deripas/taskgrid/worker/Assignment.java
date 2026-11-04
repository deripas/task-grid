package io.github.deripas.taskgrid.worker;

/**
 * Outcome of {@link Worker#assign}.
 */
public sealed interface Assignment {
    /**
     * The attempt is being executed; its result comes through {@link AttemptCallback}.
     */
    record Accepted() implements Assignment {
    }

    /**
     * The attempt was not taken (e.g. no handler for its kind); no result will come. What to do next is up to the queue.
     */
    record Rejected(String reason) implements Assignment {
    }
}
