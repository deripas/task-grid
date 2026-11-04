package io.github.deripas.taskgrid.worker;

import io.github.deripas.taskgrid.task.Attempt;

import java.util.concurrent.CompletionStage;

/**
 * Executes attempts. Two phases: {@link #assign} only hands the attempt over; its result is reported later through
 * the {@link AttemptCallback} given to the implementation. A future of the result would not survive a cluster:
 * the result may arrive at a node other than the one that assigned the attempt.
 */
public interface Worker {
    /**
     * Task meta: type of work, selects the handlers able to execute the task.
     */
    String KIND = "kind";

    /**
     * Completes when the attempt is accepted for execution or rejected.
     */
    CompletionStage<Assignment> assign(Attempt attempt);
}
