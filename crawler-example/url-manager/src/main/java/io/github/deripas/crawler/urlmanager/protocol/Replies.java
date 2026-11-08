package io.github.deripas.crawler.urlmanager.protocol;

import com.google.protobuf.ByteString;
import io.github.deripas.crawler.urlmanager.fetch.FetchOutcome;
import io.github.deripas.crawler.urlmanager.fetch.Page;
import io.github.deripas.crawler.urlmanager.v1.FetchReply;
import io.github.deripas.crawler.urlmanager.v1.Header;
import io.github.deripas.crawler.urlmanager.v1.Refusal;

import java.io.IOException;

/**
 * Replies of the protocol {@code crawler.urlmanager.v1} ({@code FetchReply}), the same over every transport.
 */
public final class Replies {
    /**
     * Content type of the messages of the protocol.
     */
    public static final String CONTENT_TYPE = "application/x-protobuf";

    private Replies() {
    }

    /**
     * Reads the outcome into a reply; the body of a page is read out and closed.
     */
    public static FetchReply reply(String id, FetchOutcome outcome) throws IOException {
        return switch (outcome) {
            case FetchOutcome.Fetched fetched -> FetchReply.newBuilder().setRequestId(id).setPage(page(fetched.page())).build();
            case FetchOutcome.Failed failed -> refusal(id, Refusal.Code.CODE_FAILED, failed.error());
            case FetchOutcome.TimedOut ignored -> refusal(id, Refusal.Code.CODE_TIMEOUT, "no page within the timeout");
            case FetchOutcome.Busy ignored -> refusal(id, Refusal.Code.CODE_BUSY, "too many requests in work");
        };
    }

    public static FetchReply refusal(
        String id,
        Refusal.Code code,
        String message
    ) {
        return FetchReply.newBuilder()
            .setRequestId(id)
            .setRefusal(Refusal.newBuilder().setCode(code).setMessage(message == null ? "" : message))
            .build();
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
