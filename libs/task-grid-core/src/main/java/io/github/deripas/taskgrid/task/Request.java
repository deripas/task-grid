package io.github.deripas.taskgrid.task;

import java.util.Map;
import java.util.Objects;

/**
 * One request for a task: who asked ({@link RequestId}) and small strings of this requester that come back with
 * the result, e.g. the correlation id of its transport. Several requests of the same task are done once, each gets
 * the result with its own tags; the meta of the task is of the work and is the same for all of them.
 *
 * @param tags immutable copy
 */
public record Request(
    RequestId id,
    Map<String, String> tags
) {
    public Request {
        Objects.requireNonNull(id, "id");
        tags = Map.copyOf(tags);
    }

    public static Request of(RequestId id) {
        return new Request(id, Map.of());
    }
}
