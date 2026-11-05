package io.github.deripas.crawler.urlmanager.amqp;

import io.github.deripas.crawler.urlmanager.fetch.FetchService;
import io.github.deripas.crawler.urlmanager.v1.FetchReply;
import io.github.deripas.crawler.urlmanager.v1.Refusal;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.Executor;

/**
 * For the in-memory queue, until it is gone (see {@link FetchRequests}): the request waits for its page on a thread of
 * {@code executor} (virtual threads) and is acknowledged after its reply is confirmed by the broker; an unconfirmed
 * reply puts the request back. The queue is not shared, so the requests of a host must come to one url-manager:
 * a consistent-hash exchange of RabbitMQ by host. The prefetch of the consumer bounds the requests in work.
 */
@Slf4j
@RequiredArgsConstructor
public final class AckAfterReplyRequests implements FetchRequests {
    private final FetchService service;
    private final ReplyPublisher replies;
    private final Executor executor;
    private final Duration defaultTimeout;
    private final Duration maxTimeout;

    @Override
    public void handle(byte[] body, Delivery delivery) {
        executor.execute(() -> process(body, delivery));
    }

    private void process(byte[] body, Delivery delivery) {
        if (!Requests.hasReplyTo(delivery)) {
            delivery.ack();
            return;
        }
        switch (Requests.parse(body, delivery)) {
            case Requests.Parsed.Invalid invalid -> {
                log.info("Fetch request refused: request={} message={}", invalid.id(), invalid.message());
                send(delivery, Requests.refusal(invalid.id(), Refusal.Code.CODE_INVALID, invalid.message()));
            }
            case Requests.Parsed.Valid valid -> send(delivery, fetch(valid));
        }
    }

    private FetchReply fetch(Requests.Parsed.Valid request) {
        try {
            service.submit(request.id(), request.url(), Map.of());
            return Requests.reply(request.id(), service.result(request.id(), request.url(), timeout(request)));
        } catch (IOException | RuntimeException e) {
            log.error("Fetch request failed in url-manager: request={} url={}", request.id(), request.url().uri(), e);
            return Requests.refusal(request.id(), Refusal.Code.CODE_FAILED, "url-manager: " + e);
        }
    }

    /**
     * Acknowledges the request when the broker has confirmed the reply, otherwise puts it back.
     */
    private void send(Delivery delivery, FetchReply reply) {
        replies.publish(delivery.replyTo(), delivery.correlationId(), reply.toByteArray()).whenComplete((ignored, error) -> {
            if (error == null) {
                delivery.ack();
            } else {
                log.warn("Reply not confirmed, request goes back to the queue: request={} error={}",
                    reply.getRequestId(), error.toString());
                delivery.requeue();
            }
        });
    }

    private Duration timeout(Requests.Parsed.Valid request) {
        if (request.timeoutMs() <= 0) {
            return defaultTimeout;
        }
        final Duration requested = Duration.ofMillis(request.timeoutMs());
        return requested.compareTo(maxTimeout) > 0
            ? maxTimeout
            : requested;
    }
}
