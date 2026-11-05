package io.github.deripas.taskgrid.dispatch;

import io.github.deripas.taskgrid.queue.ReadyCallback;
import io.github.deripas.taskgrid.queue.TaskQueue;
import io.github.deripas.taskgrid.task.Attempt;
import io.github.deripas.taskgrid.task.AttemptResult;
import io.github.deripas.taskgrid.worker.Assignment;
import io.github.deripas.taskgrid.worker.Worker;
import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.CompletionStage;

/**
 * Connects a queue to a worker: attempts the queue has ready go to the worker, results of the worker go back to
 * the queue. The same for every queue and worker implementation.
 * <ul>
 *   <li>The queue hands an attempt over as soon as its partition allows; waiting for a free handler is the worker's
 *   business (and its metric).</li>
 *   <li>An attempt the worker rejects fails its task: no worker can take it. The result goes through the queue
 *   like any other, so the partition moves on.</li>
 * </ul>
 */
@Slf4j
@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
public final class Dispatcher implements ReadyCallback {
    private final TaskQueue queue;
    private final Worker worker;

    /**
     * Subscribes the queue to the results of the worker and the worker to the ready attempts of the queue.
     * Call before submitting tasks.
     */
    public static void connect(TaskQueue queue, Worker worker) {
        worker.onComplete(queue::complete);
        queue.onReady(new Dispatcher(queue, worker));
    }

    @Override
    public void ready(Attempt attempt) {
        final CompletionStage<Assignment> assignment;
        try {
            assignment = worker.assign(attempt);
        } catch (RuntimeException e) {
            reject(attempt, "assign failed: " + e);
            return;
        }
        assignment.whenComplete((a, error) -> {
            if (error != null) {
                reject(attempt, "assign failed: " + error);
            } else if (a instanceof Assignment.Rejected rejected) {
                reject(attempt, "rejected: " + rejected.reason());
            }
        });
    }

    private void reject(Attempt attempt, String error) {
        log.debug("Attempt not taken: task={} attempt={} error={}", attempt.task().id(), attempt.number(), error);
        queue.complete(AttemptResult.failed(attempt, error)).whenComplete((ignored, e) -> {
            if (e != null) {
                log.warn("Rejection not taken by the queue, the attempt waits for its deadline: task={}", attempt.task().id(), e);
            }
        });
    }
}
