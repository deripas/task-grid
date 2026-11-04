package io.github.deripas.crawler.fetcher.http;

import com.github.tomakehurst.wiremock.WireMockServer;
import com.github.tomakehurst.wiremock.http.Fault;
import io.github.deripas.crawler.fetcher.fetch.FetchException;
import io.github.deripas.crawler.fetcher.fetch.Page;
import io.github.deripas.crawler.fetcher.fetch.TargetUrl;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.ServerSocket;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.zip.GZIPOutputStream;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static com.github.tomakehurst.wiremock.client.WireMock.ok;
import static com.github.tomakehurst.wiremock.client.WireMock.permanentRedirect;
import static com.github.tomakehurst.wiremock.client.WireMock.temporaryRedirect;
import static com.github.tomakehurst.wiremock.core.WireMockConfiguration.options;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The HTTP adapter against a fake site (WireMock): what is a page and what is a failure of the site.
 */
class ApacheFetcherTest {
    private static final int MAX_BODY = 1024;

    private final WireMockServer site = new WireMockServer(options().dynamicPort());
    private final ApacheFetcher fetcher = new ApacheFetcher(Duration.ofSeconds(2), Duration.ofMillis(500), MAX_BODY);

    @BeforeEach
    void start() {
        site.start();
    }

    @AfterEach
    void stop() throws IOException {
        fetcher.close();
        site.stop();
    }

    @Test
    void pageWithStatusHeadersAndBody() throws FetchException {
        site.stubFor(get("/page").willReturn(ok("<html>hi</html>").withHeader("Content-Type", "text/html")));

        final Page page = fetcher.fetch(url("/page"));
        assertEquals(200, page.status());
        assertEquals("<html>hi</html>", new String(page.body(), StandardCharsets.UTF_8));
        assertTrue(page.headers().contains(new Page.Header("Content-Type", "text/html")), page.headers().toString());
    }

    @Test
    void errorStatusOfTheSiteIsAPage() throws FetchException {
        site.stubFor(get("/missing").willReturn(aResponse().withStatus(404).withBody("not here")));
        site.stubFor(get("/broken").willReturn(aResponse().withStatus(500)));

        assertEquals(404, fetcher.fetch(url("/missing")).status());
        assertEquals(500, fetcher.fetch(url("/broken")).status());
    }

    @Test
    void redirectsAreFollowed() throws FetchException {
        site.stubFor(get("/old").willReturn(permanentRedirect("/temp")));
        site.stubFor(get("/temp").willReturn(temporaryRedirect("/new")));
        site.stubFor(get("/new").willReturn(ok("moved")));

        final Page page = fetcher.fetch(url("/old"));
        assertEquals(200, page.status());
        assertEquals("moved", new String(page.body(), StandardCharsets.UTF_8));
    }

    @Test
    void redirectLoopIsAFailureOfTheSite() {
        site.stubFor(get("/loop").willReturn(temporaryRedirect("/loop")));

        assertReason(FetchException.Reason.TARGET, "/loop");
    }

    @Test
    void gzipBodyIsDecoded() throws FetchException, IOException {
        final String text = "compressed text ".repeat(20);
        final ByteArrayOutputStream gzipped = new ByteArrayOutputStream();
        try (GZIPOutputStream out = new GZIPOutputStream(gzipped)) {
            out.write(text.getBytes(StandardCharsets.UTF_8));
        }
        site.stubFor(get("/gz").willReturn(ok().withHeader("Content-Encoding", "gzip").withBody(gzipped.toByteArray())));

        final Page page = fetcher.fetch(url("/gz"));
        assertEquals(text, new String(page.body(), StandardCharsets.UTF_8));
    }

    @Test
    void bodyAtTheLimitIsFetched() throws FetchException {
        site.stubFor(get("/exact").willReturn(ok().withBody(new byte[MAX_BODY])));

        assertArrayEquals(new byte[MAX_BODY], fetcher.fetch(url("/exact")).body());
    }

    @Test
    void bodyAboveTheLimitFails() {
        site.stubFor(get("/large").willReturn(ok().withBody(new byte[MAX_BODY + 1])));
        // without Content-Length: the limit is found while reading
        site.stubFor(get("/chunked").willReturn(ok().withBody(new byte[MAX_BODY * 4]).withChunkedDribbleDelay(4, 10)));

        assertReason(FetchException.Reason.TOO_LARGE, "/large");
        assertReason(FetchException.Reason.TOO_LARGE, "/chunked");
    }

    @Test
    void slowResponseIsAFailureOfTheSite() {
        site.stubFor(get("/slow").willReturn(ok("late").withFixedDelay(2000)));

        assertReason(FetchException.Reason.TARGET, "/slow");
    }

    @Test
    void brokenConnectionIsAFailureOfTheSite() {
        site.stubFor(get("/reset").willReturn(aResponse().withFault(Fault.CONNECTION_RESET_BY_PEER)));
        site.stubFor(get("/garbage").willReturn(aResponse().withFault(Fault.RANDOM_DATA_THEN_CLOSE)));

        assertReason(FetchException.Reason.TARGET, "/reset");
        assertReason(FetchException.Reason.TARGET, "/garbage");
    }

    @Test
    void refusedConnectionIsAFailureOfTheSite() throws IOException {
        final int closed;
        try (ServerSocket socket = new ServerSocket(0)) {
            closed = socket.getLocalPort();
        }
        final FetchException e = assertThrows(FetchException.class,
            () -> fetcher.fetch(TargetUrl.parse("http://localhost:" + closed + "/")));
        assertEquals(FetchException.Reason.TARGET, e.getReason());
    }

    @Test
    void unknownHostIsAFailureOfTheSite() {
        final FetchException e = assertThrows(FetchException.class,
            () -> fetcher.fetch(TargetUrl.parse("http://no-such-host.invalid/")));
        assertEquals(FetchException.Reason.TARGET, e.getReason());
        assertTrue(e.getMessage().startsWith("UnknownHostException"), e.getMessage());
    }

    private void assertReason(FetchException.Reason reason, String path) {
        final FetchException e = assertThrows(FetchException.class, () -> fetcher.fetch(url(path)));
        assertEquals(reason, e.getReason(), e.getMessage());
    }

    private TargetUrl url(String path) throws FetchException {
        return TargetUrl.parse(site.baseUrl() + path);
    }
}
