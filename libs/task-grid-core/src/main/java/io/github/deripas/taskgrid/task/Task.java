package io.github.deripas.taskgrid.task;

import java.util.Map;
import java.util.Objects;

/**
 * Unit of work: what to do ({@code payload}) and how ({@code meta}).
 * <p>
 * Properties used by particular components travel in {@code meta} under keys declared by the component they belong to,
 * e.g. {@code Worker.KIND}, {@code TaskQueue.PARTITION_KEY}, {@code Worker.AFFINITY_KEY}. The whole meta is passed to
 * the handler and copied into the result. Who asked for the task and where its result goes is not the task: see
 * {@link Request}.
 *
 * @param id      identity of the work: the queue does the same id once, however many requests ask for it while it is
 *                there; {@link #of} - the hash of the payload, so that the same request of several clients is one task
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

    /**
     * A task identified by its payload (its hash): requests with the same payload are one task. The meta is not part
     * of the identity - the meta of the first request stays.
     */
    public static Task of(Blob payload, Map<String, String> meta) {
        return new Task(payload.hash(), payload, meta);
    }
}
