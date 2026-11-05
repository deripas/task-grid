package io.github.deripas.taskgrid.queue;

import io.github.deripas.taskgrid.task.AttemptResult;

import java.time.Duration;
import java.util.Objects;

/**
 * The pause of a partition after the result of an attempt, before its next attempt (crawl-delay of a host): finish
 * to start, however long the attempt took. Gets the result, so that an application can pause longer after a refusal
 * of the site (429) or take the delay from robots.txt. Every result counts, retries wait too.
 * <p>
 * Called by the queue on its thread: must not block.
 */
public interface DelayPolicy {
    /**
     * No pauses.
     */
    DelayPolicy NONE = (key, result) -> Duration.ZERO;

    /**
     * Zero or negative - no pause.
     *
     * @param key partition key of the task
     */
    Duration after(String key, AttemptResult result);

    /**
     * The same pause for every key and result.
     */
    static DelayPolicy fixed(Duration delay) {
        Objects.requireNonNull(delay, "delay");
        return (key, result) -> delay;
    }
}
