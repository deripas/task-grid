package io.github.deripas.taskgrid.task;

import java.util.Map;
import java.util.Objects;

/**
 * Unit of work submitted by a client.
 * <p>
 * Properties used by particular components (kind, affinity key, ...) travel in {@code meta} under keys declared by
 * those components, e.g. {@code Worker.KIND}. The whole meta is passed to the handler and copied into the result:
 * the client gets its correlation data back on any node.
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
    public Task {
        Objects.requireNonNull(id, "id");
        Objects.requireNonNull(payload, "payload");
        meta = Map.copyOf(meta);
    }
}
