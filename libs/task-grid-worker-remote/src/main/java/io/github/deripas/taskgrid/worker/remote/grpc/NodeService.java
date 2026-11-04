package io.github.deripas.taskgrid.worker.remote.grpc;

import io.github.deripas.taskgrid.task.AttemptResult;
import io.github.deripas.taskgrid.task.AttemptStatus;
import io.github.deripas.taskgrid.task.Blob;
import io.github.deripas.taskgrid.worker.remote.Session;
import io.github.deripas.taskgrid.worker.remote.Sidecars;
import io.github.deripas.taskgrid.worker.remote.v1.CompleteRequest;
import io.github.deripas.taskgrid.worker.remote.v1.CompleteResponse;
import io.github.deripas.taskgrid.worker.remote.v1.Hello;
import io.github.deripas.taskgrid.worker.remote.v1.NodeGrpc;
import io.github.deripas.taskgrid.worker.remote.v1.NodeMessage;
import io.github.deripas.taskgrid.worker.remote.v1.SidecarMessage;
import io.grpc.Status;
import io.grpc.stub.ServerCallStreamObserver;
import io.grpc.stub.StreamObserver;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * gRPC service {@code taskgrid.worker.remote.v1.Node}: translates the protocol to {@link Sidecars} and back.
 * Register it in the gRPC server of the application.
 */
@Slf4j
@RequiredArgsConstructor
public final class NodeService extends NodeGrpc.NodeImplBase {
    private final Sidecars sidecars;

    @Override
    public StreamObserver<SidecarMessage> connect(StreamObserver<NodeMessage> responses) {
        final Inbound inbound = new Inbound(new StreamChannel(responses));
        // after the half-close (drain) the end of the call is seen only here
        ((ServerCallStreamObserver<NodeMessage>) responses).setOnCancelHandler(inbound::cancelled);
        return inbound;
    }

    @Override
    public void complete(CompleteRequest request, StreamObserver<CompleteResponse> responses) {
        final AttemptStatus status = toDomain(request.getStatus());
        if (status == null || request.getWorkerId().isBlank() || request.getTaskId().isBlank() || request.getAttempt() < 1) {
            responses.onError(Status.INVALID_ARGUMENT
                .withDescription("worker_id, task_id, attempt >= 1 and status are required")
                .asRuntimeException());
            return;
        }
        sidecars.complete(request.getWorkerId(), toDomain(request, status));
        responses.onNext(CompleteResponse.getDefaultInstance());
        responses.onCompleted();
    }

    /**
     * CompleteRequest -> AttemptResult: the protocol is translated to the domain here, at the boundary.
     */
    private static AttemptResult toDomain(CompleteRequest request, AttemptStatus status) {
        final Blob body = status == AttemptStatus.DONE
            ? Blob.of(request.getBody().toByteArray())
            : null;
        final String error = status == AttemptStatus.DONE
            ? null
            : request.getError();
        return new AttemptResult(request.getTaskId(), request.getAttempt(), status, body, error);
    }

    /**
     * {@code null} - not set or unknown to this version of the protocol.
     */
    private static AttemptStatus toDomain(io.github.deripas.taskgrid.worker.remote.v1.Status status) {
        return switch (status) {
            case DONE -> AttemptStatus.DONE;
            case FAILED -> AttemptStatus.FAILED;
            case RETRY -> AttemptStatus.RETRY;
            case STATUS_UNSPECIFIED, UNRECOGNIZED -> null;
        };
    }

    /**
     * Messages of one Connect stream from the sidecar; gRPC delivers them and the cancellation one at a time.
     */
    @RequiredArgsConstructor
    private final class Inbound implements StreamObserver<SidecarMessage> {
        private final StreamChannel channel;
        private Session session;

        @Override
        public void onNext(SidecarMessage message) {
            switch (message.getMsgCase()) {
                case HELLO -> hello(message.getHello());
                case CREDIT -> {
                    if (session == null) {
                        violation("hello must be the first message");
                    } else {
                        session.credit(message.getCredit().getLimit(), message.getCredit().getVersion());
                    }
                }
                case MSG_NOT_SET -> violation("empty message");
            }
        }

        @Override
        public void onError(Throwable error) {
            if (session != null) {
                session.closed("stream failed: " + Status.fromThrowable(error));
            }
        }

        /**
         * Half-close: the sidecar drains; the node ends the stream after the results of the attempts in progress.
         */
        @Override
        public void onCompleted() {
            if (session != null) {
                session.drain();
            } else {
                channel.close();
            }
        }

        void cancelled() {
            if (session != null) {
                session.closed("cancelled by sidecar");
            }
        }

        private void hello(Hello hello) {
            if (session != null) {
                violation("repeated hello");
            } else if (hello.getWorkerId().isBlank() || hello.getKind().isBlank() || hello.getCapacity() < 1) {
                violation("worker_id, kind and capacity >= 1 are required");
            } else {
                session = sidecars.connect(hello.getWorkerId(), hello.getKind(), hello.getCapacity(), channel);
            }
        }

        private void violation(String description) {
            log.warn("Protocol violation, closing the session: {}", description);
            channel.fail(Status.INVALID_ARGUMENT.withDescription(description));
            if (session != null) {
                session.closed("protocol violation: " + description);
            }
        }
    }
}
