package io.github.deripas.taskgrid.task;

import java.util.Objects;

/**
 * One execution of a task.
 *
 * @param number attempt number, from 1
 */
public record Attempt(Task task, int number) {
    public Attempt {
        Objects.requireNonNull(task, "task");
        if (number < 1) {
            throw new IllegalArgumentException("attempt number must be >= 1: " + number);
        }
    }
}
