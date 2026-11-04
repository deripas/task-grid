package io.github.deripas.crawler.fetcher.grpc;

import io.github.deripas.crawler.fetcher.fetch.FetchException;
import io.github.deripas.crawler.fetcher.fetch.Fetcher;
import io.github.deripas.crawler.fetcher.fetch.Page;
import io.github.deripas.crawler.fetcher.v1.FetchRequest;
import io.github.deripas.crawler.fetcher.v1.FetchResponse;
import io.github.deripas.crawler.fetcher.v1.FetcherGrpc;
import io.github.deripas.crawler.fetcher.v1.Header;
import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * The gRPC adapter over a fake port: proto mapping and the status of every failure (the table of fetcher.proto).
 */
class FetcherServiceTest {
    private Server server;
    private ManagedChannel channel;

    @AfterEach
    void stop() throws InterruptedException {
        channel.shutdownNow().awaitTermination(5, TimeUnit.SECONDS);
        server.shutdownNow().awaitTermination(5, TimeUnit.SECONDS);
    }

    @Test
    void pageIsTranslatedToTheResponse() throws IOException {
        final FetcherGrpc.FetcherBlockingStub stub = start(url -> new Page(404,
            List.of(new Page.Header("Content-Type", "text/html"), new Page.Header("Set-Cookie", "a=1")),
            ("not found: " + url.uri()).getBytes(StandardCharsets.UTF_8)));

        final FetchResponse response = stub.fetch(FetchRequest.newBuilder().setUrl("https://example.com/x").build());
        assertEquals(404, response.getStatus());
        assertEquals("not found: https://example.com/x", response.getBody().toStringUtf8());
        assertEquals(List.of(
            Header.newBuilder().setName("Content-Type").setValue("text/html").build(),
            Header.newBuilder().setName("Set-Cookie").setValue("a=1").build()), response.getHeadersList());
    }

    @Test
    void invalidUrlIsInvalidArgument() throws IOException {
        final FetcherGrpc.FetcherBlockingStub stub = start(url -> {
            throw new AssertionError("not called");
        });
        assertEquals(Status.Code.INVALID_ARGUMENT, code(stub, "ftp://example.com"));
    }

    @Test
    void failuresAreStatuses() throws IOException {
        assertEquals(Status.Code.FAILED_PRECONDITION, code(start(failing(FetchException.Reason.TARGET)), "http://example.com"));
        assertEquals(Status.Code.OUT_OF_RANGE, code(start(failing(FetchException.Reason.TOO_LARGE)), "http://example.com"));
    }

    @Test
    void failureOfTheFetcherIsInternal() throws IOException {
        final FetcherGrpc.FetcherBlockingStub stub = start(url -> {
            throw new IllegalStateException("bug");
        });
        assertEquals(Status.Code.INTERNAL, code(stub, "http://example.com"));
    }

    private FetcherGrpc.FetcherBlockingStub start(Fetcher fetcher) throws IOException {
        final String name = InProcessServerBuilder.generateName();
        server = InProcessServerBuilder.forName(name).addService(new FetcherService(fetcher)).build().start();
        channel = InProcessChannelBuilder.forName(name).build();
        return FetcherGrpc.newBlockingStub(channel);
    }

    private static Fetcher failing(FetchException.Reason reason) {
        return url -> {
            throw new FetchException(reason, "test");
        };
    }

    private static Status.Code code(FetcherGrpc.FetcherBlockingStub stub, String url) {
        final StatusRuntimeException e = assertThrows(StatusRuntimeException.class,
            () -> stub.fetch(FetchRequest.newBuilder().setUrl(url).build()));
        return e.getStatus().getCode();
    }
}
