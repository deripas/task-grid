package io.github.deripas.taskgrid.worker.remote;

/**
 * Load of the sidecars of one kind as seen by this node: the signals for sizing a pool.
 *
 * @param waiting  attempts waiting for a free slot: tasks stand idle because the pool is too small (scale up)
 * @param busy     attempts in progress in the sessions
 * @param capacity slots of the sessions; {@code busy / capacity} - utilization of the pool
 */
public record Load(
    int waiting,
    int busy,
    int capacity
) {
    public static final Load EMPTY = new Load(0, 0, 0);
}
