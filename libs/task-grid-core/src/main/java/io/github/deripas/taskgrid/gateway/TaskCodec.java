package io.github.deripas.taskgrid.gateway;

import io.github.deripas.taskgrid.task.Task;
import io.github.deripas.taskgrid.task.TaskResult;

/**
 * The application behind the gateways: what a payload of a client asks for, what the client gets back. The gateways
 * know neither: they are bytes of the protocol of the application, the same over every transport.
 */
public interface TaskCodec {
    /**
     * Why a request got no result: the reply of a transport that answers with a message ({@link #refusal}).
     */
    enum Refusal {
        /**
         * The payload is not a request of the application.
         */
        INVALID,
        /**
         * The gateway failed (the queue, blob storage): the request may be sent again.
         */
        FAILED,
        /**
         * No result within the time the client waits.
         */
        TIMEOUT,
        /**
         * The box of the client is full: too many requests in work.
         */
        BUSY
    }

    /**
     * The task of a payload; tasks with the same payload are one task of the queue ({@link Task#of}). Called on the
     * thread of the transport: no blocking I/O.
     *
     * @throws InvalidPayloadException the payload is not a request of the application
     */
    Task task(byte[] payload) throws InvalidPayloadException;

    /**
     * The reply of a final result to the request with the id (of its transport: the correlation id of a message, the
     * id of a gRPC request). May block (e.g. reads a large result from blob storage): called on an executor of the
     * gateway. A failure of the application (a result that cannot be read) is a reply too.
     */
    byte[] reply(String id, TaskResult result);

    /**
     * The reply to a request that got no result.
     */
    byte[] refusal(
        String id,
        Refusal refusal,
        String message
    );
}
