package io.github.deripas.taskgrid.gateway.amqp;

import io.github.deripas.taskgrid.gateway.Call;
import io.github.deripas.taskgrid.gateway.Gateway;
import io.github.deripas.taskgrid.gateway.InvalidPayloadException;
import io.github.deripas.taskgrid.gateway.TaskCodec;
import io.github.deripas.taskgrid.task.Task;
import lombok.extern.slf4j.Slf4j;

import java.time.Duration;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;

/**
 * {@link Ack#AFTER_REPLY}: the request waits for its result on a thread of the executor (virtual threads), through the
 * box {@value #CLIENT} of this node ({@link Gateway#call}), and is acknowledged after its reply is confirmed by the
 * broker; an unconfirmed reply puts the request back. The prefetch of the consumer bounds the requests in work.
 */
@Slf4j
final class AckAfterReply extends Requests {
    /**
     * The client of the requests waiting on this node: its box in the mailbox.
     */
    static final String CLIENT = "amqp";

    private final Gateway gateway;
    private final Executor executor;
    private final Duration timeout;

    AckAfterReply(
        Gateway gateway,
        TaskCodec codec,
        ReplyPublisher replies,
        Executor executor,
        Duration timeout
    ) {
        super(codec, replies);
        this.gateway = gateway;
        this.executor = executor;
        this.timeout = timeout;
    }

    @Override
    void handle(byte[] body, Delivery delivery) {
        executor.execute(() -> process(body, delivery));
    }

    private void process(byte[] body, Delivery delivery) {
        if (!hasReplyTo(delivery)) {
            delivery.ack();
            return;
        }
        final String id = id(body);
        final String replyId = replyId(delivery, id);
        final Task task;
        try {
            task = codec.task(body);
        } catch (InvalidPayloadException e) {
            log.info("Request refused: correlation_id={} message={}", delivery.correlationId(), e.getMessage());
            send(delivery, codec.refusal(replyId, TaskCodec.Refusal.INVALID, e.getMessage()));
            return;
        }
        send(delivery, reply(replyId, gateway.call(CLIENT, id, task, timeout)));
    }

    private byte[] reply(String replyId, CompletionStage<Call> call) {
        try {
            return switch (call.toCompletableFuture().join()) {
                case Call.Done done -> codec.reply(replyId, done.result());
                case Call.TimedOut ignored -> codec.refusal(replyId, TaskCodec.Refusal.TIMEOUT, "no result within " + timeout);
                case Call.Busy ignored -> codec.refusal(replyId, TaskCodec.Refusal.BUSY, "too many requests in work");
                case Call.Rejected rejected -> codec.refusal(replyId, TaskCodec.Refusal.INVALID, rejected.reason());
            };
        } catch (RuntimeException e) {
            log.error("Request failed in the gateway: correlation_id={}", replyId, e);
            return codec.refusal(replyId, TaskCodec.Refusal.FAILED, "gateway: " + e);
        }
    }
}
