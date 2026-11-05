package io.github.deripas.taskgrid.worker.delayed;

import io.github.deripas.taskgrid.task.AttemptResult;

import java.time.Duration;

/**
 * Port: the pause of an affinity key after a result, before its next attempt (crawl-delay of a host). Gets the result,
 * so that an application can pause longer after a refusal of the site (429) or take the delay from robots.txt.
 * Called on the thread that reports the result: must not block.
 */
public interface DelayPolicy {
    /**
     * Zero or negative - no pause.
     */
    Duration after(String key, AttemptResult result);
}
