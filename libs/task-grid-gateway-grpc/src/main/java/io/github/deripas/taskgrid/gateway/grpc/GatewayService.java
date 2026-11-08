package io.github.deripas.taskgrid.gateway.grpc;

import io.github.deripas.taskgrid.gateway.Gateway;
import io.github.deripas.taskgrid.gateway.InvalidPayloadException;
import io.github.deripas.taskgrid.gateway.Submission;
import io.github.deripas.taskgrid.gateway.TaskCodec;
import io.github.deripas.taskgrid.gateway.grpc.v1.AckReply;
import io.github.deripas.taskgrid.gateway.grpc.v1.AckRequest;
import io.github.deripas.taskgrid.gateway.grpc.v1.Result;
import io.github.deripas.taskgrid.gateway.grpc.v1.SubmitReply;
import io.github.deripas.taskgrid.gateway.grpc.v1.SubmitRequest;
import io.github.deripas.taskgrid.gateway.grpc.v1.SubscribeRequest;
import io.github.deripas.taskgrid.gateway.grpc.v1.TaskGatewayGrpc;
import io.github.deripas.taskgrid.mailbox.Box;
import io.github.deripas.taskgrid.task.RequestId;
import io.github.deripas.taskgrid.task.Task;
import io.grpc.Status;
import io.grpc.stub.ServerCallStreamObserver;
import io.grpc.stub.StreamObserver;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

/**
 * {@code taskgrid.gateway.v1.TaskGateway} over the {@link Gateway} of task-grid-core; register it in the gRPC server
 * of the node. Payloads and replies are those of the {@link TaskCodec} of the application; replies are made on the
 * executor (they may read blob storage): virtual threads.
 */
@Slf4j
@RequiredArgsConstructor
public final class GatewayService extends TaskGatewayGrpc.TaskGatewayImplBase {
    private final Gateway gateway;
    private final TaskCodec codec;
    private final Executor executor;

    @Override
    public void submit(SubmitRequest request, StreamObserver<SubmitReply> response) {
        final RequestId id;
        try {
            id = new RequestId(request.getClient(), request.getConsumer(), request.getId());
        } catch (IllegalArgumentException e) {
            response.onError(Status.INVALID_ARGUMENT.withDescription(e.getMessage()).asRuntimeException());
            return;
        }
        final Task task;
        try {
            task = codec.task(request.getPayload().toByteArray());
        } catch (InvalidPayloadException e) {
            response.onError(Status.INVALID_ARGUMENT.withDescription(e.getMessage()).asRuntimeException());
            return;
        }
        gateway.submit(id, task).whenComplete((submission, error) -> {
            if (error != null) {
                response.onError(Status.UNAVAILABLE.withDescription("not queued, retry: " + error).asRuntimeException());
                return;
            }
            switch (submission) {
                case Submission.Accepted ignored -> {
                    response.onNext(SubmitReply.getDefaultInstance());
                    response.onCompleted();
                }
                case Submission.Busy ignored -> response.onError(Status.RESOURCE_EXHAUSTED
                    .withDescription("the box " + Box.of(id) + " is full: acknowledge results first").asRuntimeException());
                case Submission.Rejected rejected -> response.onError(Status.INVALID_ARGUMENT
                    .withDescription(rejected.reason()).asRuntimeException());
            }
        });
    }

    @Override
    public void subscribe(SubscribeRequest request, StreamObserver<Result> response) {
        final Box box;
        try {
            box = new Box(request.getClient(), request.getConsumer());
        } catch (IllegalArgumentException e) {
            response.onError(Status.INVALID_ARGUMENT.withDescription(e.getMessage()).asRuntimeException());
            return;
        }
        gateway.subscribe(box, new ResultStream(box, (ServerCallStreamObserver<Result>) response, codec, executor));
    }

    /**
     * Acknowledgements of any boxes; the reply once all of them are done.
     */
    @Override
    public StreamObserver<AckRequest> ack(StreamObserver<AckReply> response) {
        return new StreamObserver<>() {
            private CompletableFuture<Void> acks = CompletableFuture.completedFuture(null);

            @Override
            public void onNext(AckRequest request) {
                final Box box;
                try {
                    box = new Box(request.getClient(), request.getConsumer());
                } catch (IllegalArgumentException e) {
                    log.debug("Ack of a bad box ignored: {}", e.getMessage());
                    return;
                }
                final CompletableFuture<Void> ack = gateway.ack(box, request.getIdsList()).toCompletableFuture();
                acks = CompletableFuture.allOf(acks, ack);
            }

            @Override
            public void onError(Throwable error) {
                log.debug("Ack stream broken: {}", error.toString());
            }

            @Override
            public void onCompleted() {
                acks.whenComplete((ignored, error) -> {
                    if (error == null) {
                        response.onNext(AckReply.getDefaultInstance());
                        response.onCompleted();
                    } else {
                        response.onError(Status.UNAVAILABLE.withDescription("not acknowledged: " + error).asRuntimeException());
                    }
                });
            }
        };
    }
}
