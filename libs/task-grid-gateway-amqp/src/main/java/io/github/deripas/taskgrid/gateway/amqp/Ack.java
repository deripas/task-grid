package io.github.deripas.taskgrid.gateway.amqp;

/**
 * When a request message is acknowledged - which way of handling the requests get.
 */
public enum Ack {
    /**
     * After its reply is confirmed by the broker; the request waits for its result on the node that took it (its box
     * of this node). A request of a node that stopped stays in the queue and is delivered again.
     */
    AFTER_REPLY,

    /**
     * As soon as its task is queued; the reply is published by the result callback of the queue, wherever the task
     * finishes. For a queue that keeps its tasks: with the in-memory one a stop loses acknowledged requests.
     */
    AFTER_SUBMIT
}
