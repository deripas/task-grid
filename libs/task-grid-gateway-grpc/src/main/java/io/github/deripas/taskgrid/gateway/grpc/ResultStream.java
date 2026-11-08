package io.github.deripas.taskgrid.gateway.grpc;

import com.google.protobuf.ByteString;
import io.github.deripas.taskgrid.gateway.TaskCodec;
import io.github.deripas.taskgrid.gateway.grpc.v1.Result;
import io.github.deripas.taskgrid.mailbox.Box;
import io.github.deripas.taskgrid.mailbox.Reply;
import io.github.deripas.taskgrid.mailbox.SupersededException;
import io.grpc.Status;
import io.grpc.stub.ServerCallStreamObserver;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.Executor;
import java.util.concurrent.Flow;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * A subscription of a box as a server stream: one reply at a time - made on the executor, sent, and only then the
 * next one is requested from the box, when the stream is ready (flow control of gRPC). Calls of the stream are
 * serialized by its lock: replies, the end of the subscription and the cancellation come from different threads.
 */
@Slf4j
final class ResultStream implements Flow.Subscriber<Reply> {
    private final Box box;
    private final ServerCallStreamObserver<Result> stream;
    private final TaskCodec codec;
    private final Executor executor;
    /**
     * The stream was not ready after a reply: the next one is requested by its ready handler.
     */
    private final AtomicBoolean waitingForReady = new AtomicBoolean();
    private volatile Flow.Subscription subscription;
    /**
     * Guarded by {@code this}.
     */
    private boolean done;

    ResultStream(
        Box box,
        ServerCallStreamObserver<Result> stream,
        TaskCodec codec,
        Executor executor
    ) {
        this.box = box;
        this.stream = stream;
        this.codec = codec;
        this.executor = executor;
        stream.setOnReadyHandler(this::ready);
        stream.setOnCancelHandler(this::cancelled);
    }

    @Override
    public void onSubscribe(Flow.Subscription subscription) {
        this.subscription = subscription;
        synchronized (this) {
            if (done) {
                subscription.cancel();
                return;
            }
        }
        subscription.request(1);
    }

    @Override
    public void onNext(Reply reply) {
        executor.execute(() -> send(reply));
    }

    @Override
    public void onError(Throwable error) {
        final Status status = error instanceof SupersededException
            ? Status.ABORTED.withDescription(error.getMessage())
            : Status.UNAVAILABLE.withDescription("subscription of " + box + " ended: " + error);
        end(status);
    }

    @Override
    public void onComplete() {
        synchronized (this) {
            if (!done) {
                done = true;
                stream.onCompleted();
            }
        }
    }

    private void send(Reply reply) {
        final byte[] payload;
        try {
            payload = codec.reply(reply.id(), reply.result());
        } catch (RuntimeException e) {
            log.error("Reply not made, the stream of {} ends: request={}", box, reply.id(), e);
            subscription.cancel();
            end(Status.INTERNAL.withDescription("reply to " + reply.id() + " not made: " + e));
            return;
        }
        synchronized (this) {
            if (done) {
                return;
            }
            stream.onNext(Result.newBuilder().setId(reply.id()).setPayload(ByteString.copyFrom(payload)).build());
        }
        waitingForReady.set(true);
        if (stream.isReady()) {
            ready();
        }
    }

    private void ready() {
        if (waitingForReady.compareAndSet(true, false)) {
            subscription.request(1);
        }
    }

    private void cancelled() {
        synchronized (this) {
            done = true;
        }
        final Flow.Subscription current = subscription;
        if (current != null) {
            current.cancel();
        }
    }

    private void end(Status status) {
        synchronized (this) {
            if (done) {
                return;
            }
            done = true;
            stream.onError(status.asRuntimeException());
        }
    }
}
