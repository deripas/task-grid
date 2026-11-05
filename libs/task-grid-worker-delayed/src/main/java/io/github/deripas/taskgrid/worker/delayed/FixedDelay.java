package io.github.deripas.taskgrid.worker.delayed;

import io.github.deripas.taskgrid.task.AttemptResult;

import java.time.Duration;
import java.util.Objects;

/**
 * The same pause for every key and result.
 */
public record FixedDelay(Duration delay) implements DelayPolicy {
    public FixedDelay {
        Objects.requireNonNull(delay, "delay");
    }

    @Override
    public Duration after(String key, AttemptResult result) {
        return delay;
    }
}
