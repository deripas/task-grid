package io.github.deripas.crawler.urlmanager.amqp;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import io.github.deripas.crawler.urlmanager.fetch.FetchOutcome;
import io.github.deripas.crawler.urlmanager.fetch.InvalidUrlException;
import io.github.deripas.crawler.urlmanager.fetch.Page;
import io.github.deripas.crawler.urlmanager.fetch.PageUrl;
import io.github.deripas.crawler.urlmanager.v1.FetchReply;
import io.github.deripas.crawler.urlmanager.v1.FetchRequest;
import io.github.deripas.crawler.urlmanager.v1.Header;
import io.github.deripas.crawler.urlmanager.v1.Refusal;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.UUID;

/**
 * Protocol {@code crawler.urlmanager.v1} for both ways of handling: the request read from a message, the replies.
 */
@Slf4j
final class Requests {
    private Requests() {
    }

    /**
     * A request message as read.
     */
    sealed interface Parsed {
        /**
         * The page can be fetched.
         */
        record Valid(String id, PageUrl url, long timeoutMs) implements Parsed {
        }

        /**
         * Refused at once.
         */
        record Invalid(String id, String message) implements Parsed {
        }
    }

    static Parsed parse(byte[] body, Delivery delivery) {
        final FetchRequest request;
        try {
            request = FetchRequest.parseFrom(body);
        } catch (InvalidProtocolBufferException e) {
            return new Parsed.Invalid("", "body is not crawler.urlmanager.v1.FetchRequest: " + e.getMessage());
        }
        final String id = id(request, delivery);
        try {
            return new Parsed.Valid(id, PageUrl.parse(request.getUrl()), request.getTimeoutMs());
        } catch (InvalidUrlException e) {
            return new Parsed.Invalid(id, e.getMessage());
        }
    }

    /**
     * Reads the outcome into a reply; the body of a page is read out and closed.
     */
    static FetchReply reply(String id, FetchOutcome outcome) throws IOException {
        return switch (outcome) {
            case FetchOutcome.Fetched fetched -> FetchReply.newBuilder().setRequestId(id).setPage(page(fetched.page())).build();
            case FetchOutcome.Failed failed -> refusal(id, Refusal.Code.CODE_FAILED, failed.error());
            case FetchOutcome.TimedOut ignored -> refusal(id, Refusal.Code.CODE_TIMEOUT, "no page within the timeout");
        };
    }

    static FetchReply refusal(
        String id,
        Refusal.Code code,
        String message
    ) {
        return FetchReply.newBuilder()
            .setRequestId(id)
            .setRefusal(Refusal.newBuilder().setCode(code).setMessage(message == null ? "" : message))
            .build();
    }

    static boolean hasReplyTo(Delivery delivery) {
        if (delivery.replyTo() != null && !delivery.replyTo().isBlank()) {
            return true;
        }
        log.warn("Request without reply_to dropped: message_id={}", delivery.messageId());
        return false;
    }

    /**
     * The client's request_id, else the message_id, else a new one.
     */
    private static String id(FetchRequest request, Delivery delivery) {
        if (!request.getRequestId().isBlank()) {
            return request.getRequestId();
        }
        return delivery.messageId() != null && !delivery.messageId().isBlank()
            ? delivery.messageId()
            : UUID.randomUUID().toString();
    }

    private static io.github.deripas.crawler.urlmanager.v1.Page page(Page page) throws IOException {
        try (page) {
            return io.github.deripas.crawler.urlmanager.v1.Page.newBuilder()
                .setStatus(page.status())
                .addAllHeaders(page.headers().stream()
                    .map(h -> Header.newBuilder().setName(h.name()).setValue(h.value()).build())
                    .toList())
                .setBody(ByteString.readFrom(page.body()))
                .build();
        }
    }
}
