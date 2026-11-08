package io.github.deripas.taskgrid.mailbox;

/**
 * Ends a subscription of a box: another subscription of the same box came (the consumer reconnected, or two
 * instances use the same consumer).
 */
public final class SupersededException extends RuntimeException {
    public SupersededException(Box box) {
        super("subscription of " + box + " superseded by a newer one");
    }
}
