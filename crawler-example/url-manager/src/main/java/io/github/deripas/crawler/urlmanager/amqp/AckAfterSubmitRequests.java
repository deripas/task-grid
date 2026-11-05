package io.github.deripas.crawler.urlmanager.amqp;

import io.github.deripas.crawler.urlmanager.fetch.FetchCallback;
import io.github.deripas.crawler.urlmanager.fetch.FetchService;
import io.github.deripas.crawler.urlmanager.fetch.FinishedFetch;
import io.github.deripas.crawler.urlmanager.v1.FetchReply;
import io.github.deripas.crawler.urlmanager.v1.Refusal;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;

/**
 * The target way (see {@link FetchRequests}): the request is acknowledged as soon as its task is queued; the reply
 * address travels with the task, and the reply is sent by the result callback of the queue on the node where the task
 * finished - any url-manager of the cluster, whether or not the one that took the request is alive. Register it with
 * {@link FetchService#onFinished}.
 * <p>
 * Needs a queue that keeps its tasks and delivers results again until the reply is confirmed: with the in-memory queue
 * a stop of url-manager loses acknowledged requests. No timeout: the reply comes when the task finishes
 * ({@code timeout_ms} is not used).
 */
@Slf4j
@RequiredArgsConstructor
public final class AckAfterSubmitRequests implements FetchRequests, FetchCallback {
    static final String REPLY_TO = "amqp.reply-to";
    static final String CORRELATION_ID = "amqp.correlation-id";

    private final FetchService service;
    private final ReplyPublisher replies;
    private final Executor executor;

    @Override
    public void handle(byte[] body, Delivery delivery) {
        executor.execute(() -> process(body, delivery));
    }

    /**
     * A finished fetch: replies to the address it was submitted with; fetches of other ways have none.
     */
    @Override
    public CompletionStage<Void> finished(FinishedFetch fetch) {
        final String replyTo = fetch.tags().get(REPLY_TO);
        if (replyTo == null) {
            return CompletableFuture.completedFuture(null);
        }
        return CompletableFuture.supplyAsync(() -> reply(fetch), executor)
            .thenCompose(reply -> replies.publish(replyTo, fetch.tags().get(CORRELATION_ID), reply.toByteArray()));
    }

    private void process(byte[] body, Delivery delivery) {
        if (!Requests.hasReplyTo(delivery)) {
            delivery.ack();
            return;
        }
        switch (Requests.parse(body, delivery)) {
            case Requests.Parsed.Invalid invalid -> refuse(delivery, invalid);
            case Requests.Parsed.Valid valid -> submit(delivery, valid);
        }
    }

    private void submit(Delivery delivery, Requests.Parsed.Valid request) {
        final Map<String, String> tags = new HashMap<>();
        tags.put(REPLY_TO, delivery.replyTo());
        if (delivery.correlationId() != null) {
            tags.put(CORRELATION_ID, delivery.correlationId());
        }
        try {
            service.submit(request.id(), request.url(), tags);
            delivery.ack();
        } catch (RuntimeException e) {
            log.warn("Request not queued, goes back to the queue: request={} error={}", request.id(), e.toString());
            delivery.requeue();
        }
    }

    private void refuse(Delivery delivery, Requests.Parsed.Invalid request) {
        log.info("Fetch request refused: request={} message={}", request.id(), request.message());
        final FetchReply refusal = Requests.refusal(request.id(), Refusal.Code.CODE_INVALID, request.message());
        replies.publish(delivery.replyTo(), delivery.correlationId(), refusal.toByteArray()).whenComplete((ignored, error) -> {
            if (error == null) {
                delivery.ack();
            } else {
                delivery.requeue();
            }
        });
    }

    /**
     * The page is read here, on a thread of the executor: it may come from blob storage. A failure to read it is
     * a failure of url-manager, not of the site: the client gets CODE_FAILED.
     */
    private static FetchReply reply(FinishedFetch fetch) {
        try {
            return Requests.reply(fetch.id(), fetch.read());
        } catch (Exception e) {
            log.error("Finished fetch not read: request={}", fetch.id(), e);
            return Requests.refusal(fetch.id(), Refusal.Code.CODE_FAILED, "url-manager: " + e);
        }
    }
}
