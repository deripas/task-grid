package io.github.deripas.taskgrid.worker.remote.grpc;

import com.google.protobuf.ByteString;
import io.github.deripas.taskgrid.task.Attempt;
import io.github.deripas.taskgrid.task.Blob;
import io.github.deripas.taskgrid.worker.remote.SessionChannel;
import io.github.deripas.taskgrid.worker.remote.v1.Demand;
import io.github.deripas.taskgrid.worker.remote.v1.NodeMessage;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import lombok.RequiredArgsConstructor;

/**
 * Response stream of Connect as a {@link SessionChannel}. The engine writes attempts and demand, the service
 * ends the stream with an error on a protocol violation: writes are serialized here.
 */
@RequiredArgsConstructor
final class StreamChannel implements SessionChannel {
    private final StreamObserver<NodeMessage> out;
    private boolean closed;

    @Override
    public boolean attempt(Attempt attempt) {
        return send(NodeMessage.newBuilder().setAttempt(toProto(attempt)).build());
    }

    @Override
    public boolean demand(long target, long version) {
        return send(NodeMessage.newBuilder().setDemand(Demand.newBuilder().setTarget(target).setVersion(version)).build());
    }

    @Override
    public synchronized void close() {
        if (closed) {
            return;
        }
        closed = true;
        try {
            out.onCompleted();
        } catch (RuntimeException e) {
            // the call is already over
        }
    }

    /**
     * Ends the stream with an error status; later writes are dropped.
     */
    synchronized void fail(Status status) {
        if (closed) {
            return;
        }
        closed = true;
        try {
            out.onError(status.asRuntimeException());
        } catch (RuntimeException e) {
            // the call is already over
        }
    }

    private synchronized boolean send(NodeMessage message) {
        if (closed) {
            return false;
        }
        try {
            out.onNext(message);
            return true;
        } catch (RuntimeException e) {
            return false;
        }
    }

    /**
     * Attempt -> proto: the domain is translated to the protocol here, at the boundary.
     */
    private static io.github.deripas.taskgrid.worker.remote.v1.Attempt toProto(Attempt attempt) {
        if (!(attempt.task().payload() instanceof Blob.Inline inline)) {
            throw new IllegalArgumentException("only inline payloads are sent to sidecars: task " + attempt.task().id());
        }
        return io.github.deripas.taskgrid.worker.remote.v1.Attempt.newBuilder()
            .setTaskId(attempt.task().id())
            .setNumber(attempt.number())
            .putAllMeta(attempt.task().meta())
            .setPayload(ByteString.copyFrom(inline.bytes()))
            .build();
    }
}
