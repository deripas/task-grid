package io.github.deripas.taskgrid.mailbox;

/**
 * Answer of {@link Mailbox#reserve}: whether the task goes to the queue.
 */
public enum Reservation {
    /**
     * A new request: submit it; release the place if the submit fails.
     */
    NEW,
    /**
     * The request is reserved already and has no result yet (a client retries): submit it again, the queue keeps one
     * request per id while it is there; do not release the place if the submit fails, the earlier one may have succeeded.
     */
    PENDING,
    /**
     * The result of the request waits in the box: do not submit, the consumer gets the result.
     */
    DONE,
    /**
     * The box has {@link MailboxSettings#limit} requests: refuse the request until the consumer acknowledges results.
     */
    FULL
}
