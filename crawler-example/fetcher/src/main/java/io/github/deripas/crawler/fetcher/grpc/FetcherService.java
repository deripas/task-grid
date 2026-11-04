package io.github.deripas.crawler.fetcher.grpc;

import com.google.protobuf.ByteString;
import io.github.deripas.crawler.fetcher.fetch.FetchException;
import io.github.deripas.crawler.fetcher.fetch.Fetcher;
import io.github.deripas.crawler.fetcher.fetch.Page;
import io.github.deripas.crawler.fetcher.fetch.TargetUrl;
import io.github.deripas.crawler.fetcher.v1.FetchRequest;
import io.github.deripas.crawler.fetcher.v1.FetchResponse;
import io.github.deripas.crawler.fetcher.v1.FetcherGrpc;
import io.github.deripas.crawler.fetcher.v1.Header;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * gRPC service {@code crawler.fetcher.v1.Fetcher}: protocol &lt;-&gt; {@link Fetcher}, failures -&gt; gRPC statuses
 * (the table is in fetcher.proto). Blocking: the server runs calls on virtual threads.
 */
@Slf4j
@RequiredArgsConstructor
public final class FetcherService extends FetcherGrpc.FetcherImplBase {
    private final Fetcher fetcher;

    @Override
    public void fetch(FetchRequest request, StreamObserver<FetchResponse> responses) {
        final long started = System.nanoTime();
        try {
            final Page page = fetcher.fetch(TargetUrl.parse(request.getUrl()));
            log.info("Fetched: url={} status={} bytes={} ms={}",
                request.getUrl(), page.status(), page.body().length, (System.nanoTime() - started) / 1_000_000);
            responses.onNext(toProto(page));
            responses.onCompleted();
        } catch (FetchException e) {
            log.info("Not fetched: url={} reason={} error={}", request.getUrl(), e.getReason(), e.getMessage());
            responses.onError(toStatus(e.getReason()).withDescription(e.getMessage()).asRuntimeException());
        } catch (RuntimeException e) {
            log.error("Fetch failed: url={}", request.getUrl(), e);
            responses.onError(Status.INTERNAL.withDescription(String.valueOf(e)).asRuntimeException());
        }
    }

    /**
     * Page -&gt; proto: the domain is translated to the protocol here, at the boundary.
     */
    private static FetchResponse toProto(Page page) {
        final FetchResponse.Builder b = FetchResponse.newBuilder()
            .setStatus(page.status())
            .setBody(ByteString.copyFrom(page.body()));
        for (Page.Header h : page.headers()) {
            b.addHeaders(Header.newBuilder().setName(h.name()).setValue(h.value()));
        }
        return b.build();
    }

    private static Status toStatus(FetchException.Reason reason) {
        return switch (reason) {
            case INVALID_URL -> Status.INVALID_ARGUMENT;
            case TARGET -> Status.FAILED_PRECONDITION;
            case TOO_LARGE -> Status.OUT_OF_RANGE;
        };
    }
}
