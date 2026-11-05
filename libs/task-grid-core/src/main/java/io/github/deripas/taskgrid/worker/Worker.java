package io.github.deripas.taskgrid.worker;

import io.github.deripas.taskgrid.task.Attempt;

import java.util.concurrent.CompletionStage;

/**
 * Executes attempts. Two phases: {@link #assign} only hands the attempt over; its result is reported later to the
 * {@link AttemptCallback}s registered with {@link #onComplete}. A future of the result would not survive a cluster:
 * the result may arrive at a node other than the one that assigned the attempt.
 * <p>
 * Usable on its own: whoever assigns attempts subscribes to their results. Connected to a queue by {@code Dispatcher}.
 */
public interface Worker {
    /**
     * Task meta: type of work, selects the handlers able to execute the task.
     */
    String KIND = "kind";

    /**
     * Task meta: attempts with the same value preferably run on the same handler instance (warm connections and
     * caches); a worker that has several instances follows it, waiting a little for the preferred one. Absent - any
     * free instance. Independent of the partition of the queue: set both when both are wanted.
     */
    String AFFINITY_KEY = "affinity-key";

    /**
     * Completes when the attempt is accepted for execution or rejected.
     */
    CompletionStage<Assignment> assign(Attempt attempt);

    /**
     * Registers a receiver of the results of accepted attempts. Register before assigning attempts: a result reported
     * while there is no receiver is lost.
     */
    void onComplete(AttemptCallback callback);
}
