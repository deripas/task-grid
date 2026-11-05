package io.github.deripas.taskgrid.task;

import java.util.Map;
import java.util.Objects;

/**
 * Unit of work submitted by a client.
 * <p>
 * Properties used by particular components travel in {@code meta} under keys declared by those components,
 * e.g. {@code Worker.KIND}; a key used by several components is declared here. The whole meta is passed to the handler
 * and copied into the result: the client gets its correlation data back on any node.
 *
 * @param id      set by the client; a retried submission with the same id is the same task
 * @param payload input of the handler, opaque to task-grid
 * @param meta    small string properties; immutable copy
 */
public record Task(
    String id,
    Blob payload,
    Map<String, String> meta
) {
    /**
     * Meta: tasks with the same value run one at a time, in order (queue), preferably on the same handler
     * instance (worker). Absent - the task is independent.
     */
    public static final String AFFINITY_KEY = "affinity-key";

    public Task {
        Objects.requireNonNull(id, "id");
        Objects.requireNonNull(payload, "payload");
        meta = Map.copyOf(meta);
    }
}
