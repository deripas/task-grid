package io.github.deripas.taskgrid.gateway.amqp;

import io.github.deripas.taskgrid.gateway.InvalidPayloadException;
import io.github.deripas.taskgrid.gateway.TaskCodec;
import io.github.deripas.taskgrid.queue.ResultCallback;
import io.github.deripas.taskgrid.queue.TaskQueue;
import io.github.deripas.taskgrid.task.Request;
import io.github.deripas.taskgrid.task.RequestId;
import io.github.deripas.taskgrid.task.Task;
import io.github.deripas.taskgrid.task.TaskResult;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;

/**
 * {@link Ack#AFTER_SUBMIT}: the request is acknowledged as soon as its task is queued. The reply address is the request
 * itself - client {@value #CLIENT}, consumer - the reply_to queue, id - the hash of the message - with the correlation
 * id in its tags; the reply is published by the result callback of the queue on the node where the task finished, any
 * node of the cluster. Register it on the queue ({@link TaskQueue#onResult}) before the node takes tasks.
 */
@Slf4j
final class AckAfterSubmit extends Requests implements ResultCallback {
    /**
     * Client of the requests of this way: their results are replies to publish.
     */
    static final String CLIENT = "amqp-reply-to";
    static final String CORRELATION_ID = "correlation-id";

    private final TaskQueue queue;
    private final Executor executor;

    AckAfterSubmit(
        TaskQueue queue,
        TaskCodec codec,
        ReplyPublisher replies,
        Executor executor
    ) {
        super(codec, replies);
        this.queue = queue;
        this.executor = executor;
    }

    @Override
    void handle(byte[] body, Delivery delivery) {
        executor.execute(() -> process(body, delivery));
    }

    /**
     * The replies to the requests of this way; the reply is made on the executor (it may read blob storage).
     */
    @Override
    public CompletionStage<Void> finished(TaskResult result, List<Request> requests) {
        final List<CompletableFuture<Void>> stages = new ArrayList<>();
        for (Request request : requests) {
            if (CLIENT.equals(request.id().client())) {
                final String correlationId = request.tags().get(CORRELATION_ID);
                final String replyId = correlationId != null ? correlationId : request.id().id();
                stages.add(CompletableFuture.supplyAsync(() -> codec.reply(replyId, result), executor)
                    .thenCompose(reply -> replies.publish(request.id().consumer(), correlationId, reply))
                    .toCompletableFuture());
            }
        }
        return CompletableFuture.allOf(stages.toArray(CompletableFuture[]::new));
    }

    private void process(byte[] body, Delivery delivery) {
        if (!hasReplyTo(delivery)) {
            delivery.ack();
            return;
        }
        final String id = id(body);
        final Task task;
        try {
            task = codec.task(body);
        } catch (InvalidPayloadException e) {
            log.info("Request refused: correlation_id={} message={}", delivery.correlationId(), e.getMessage());
            send(delivery, codec.refusal(replyId(delivery, id), TaskCodec.Refusal.INVALID, e.getMessage()));
            return;
        }
        final Map<String, String> tags = delivery.correlationId() == null
            ? Map.of()
            : Map.of(CORRELATION_ID, delivery.correlationId());
        try {
            queue.submit(task, new Request(new RequestId(CLIENT, delivery.replyTo(), id), tags)).toCompletableFuture().join();
            delivery.ack();
        } catch (CompletionException e) {
            if (e.getCause() instanceof IllegalArgumentException refused) {
                send(delivery, codec.refusal(replyId(delivery, id), TaskCodec.Refusal.INVALID, refused.getMessage()));
                return;
            }
            log.warn("Request not queued, goes back to the queue: correlation_id={} error={}", delivery.correlationId(), e.toString());
            delivery.requeue();
        } catch (RuntimeException e) {
            log.warn("Request not queued, goes back to the queue: correlation_id={} error={}", delivery.correlationId(), e.toString());
            delivery.requeue();
        }
    }
}
