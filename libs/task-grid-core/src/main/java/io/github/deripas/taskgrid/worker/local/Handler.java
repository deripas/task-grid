package io.github.deripas.taskgrid.worker.local;

import io.github.deripas.taskgrid.task.Attempt;
import io.github.deripas.taskgrid.task.Blob;

/**
 * Does the work for a kind inside the JVM; called by the in-process worker on its executor, may block.
 * <p>
 * The outcome maps to the attempt status the same way a remote handler's gRPC status does in handler-sidecar:
 * <ul>
 *   <li>returns a body - {@code DONE};</li>
 *   <li>throws {@link RetryableException} - {@code RETRY}, while attempts remain;</li>
 *   <li>throws any other exception - {@code FAILED}, no retry: unknown failures are not retried blindly.</li>
 * </ul>
 */
@FunctionalInterface
public interface Handler {
    Blob handle(Attempt attempt) throws Exception;
}
