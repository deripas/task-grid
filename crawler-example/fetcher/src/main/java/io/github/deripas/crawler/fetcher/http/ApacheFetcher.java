package io.github.deripas.crawler.fetcher.http;

import io.github.deripas.crawler.fetcher.fetch.FetchException;
import io.github.deripas.crawler.fetcher.fetch.Fetcher;
import io.github.deripas.crawler.fetcher.fetch.Page;
import io.github.deripas.crawler.fetcher.fetch.TargetUrl;
import lombok.extern.slf4j.Slf4j;
import org.apache.hc.client5.http.classic.methods.HttpGet;
import org.apache.hc.client5.http.config.ConnectionConfig;
import org.apache.hc.client5.http.config.RequestConfig;
import org.apache.hc.client5.http.impl.classic.CloseableHttpClient;
import org.apache.hc.client5.http.impl.classic.HttpClients;
import org.apache.hc.client5.http.impl.io.PoolingHttpClientConnectionManagerBuilder;
import org.apache.hc.core5.http.ClassicHttpResponse;
import org.apache.hc.core5.http.Header;
import org.apache.hc.core5.http.HttpEntity;
import org.apache.hc.core5.util.Timeout;

import java.io.IOException;
import java.io.InputStream;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

/**
 * {@link Fetcher} over Apache HttpClient 5 (classic, blocking - callers run on virtual threads).
 * <ul>
 *   <li>No retries and no cookies: one call is one exchange with the site.</li>
 *   <li>Redirects are followed (up to {@value #MAX_REDIRECTS}); the body is decoded from Content-Encoding.</li>
 *   <li>The body is read up to {@code maxBody} bytes; a larger one fails the fetch.</li>
 *   <li>Any I/O failure of the exchange is a failure of the site ({@link FetchException.Reason#TARGET}).</li>
 * </ul>
 */
@Slf4j
public final class ApacheFetcher implements Fetcher, AutoCloseable {
    private static final int MAX_REDIRECTS = 10;
    private static final int MAX_CONNECTIONS = 1000;

    private final CloseableHttpClient client;
    private final int maxBody;

    /**
     * @param connectTimeout  limit of establishing a connection
     * @param responseTimeout limit of waiting for data of the response (socket timeout)
     * @param maxBody         limit of the decoded body, bytes
     */
    public ApacheFetcher(
        Duration connectTimeout,
        Duration responseTimeout,
        int maxBody
    ) {
        this.maxBody = maxBody;
        final ConnectionConfig connection = ConnectionConfig.custom()
            .setConnectTimeout(Timeout.of(connectTimeout))
            .setSocketTimeout(Timeout.of(responseTimeout))
            .build();
        final RequestConfig request = RequestConfig.custom()
            .setResponseTimeout(Timeout.of(responseTimeout))
            .setRedirectsEnabled(true)
            .setMaxRedirects(MAX_REDIRECTS)
            .build();
        this.client = HttpClients.custom()
            .setConnectionManager(PoolingHttpClientConnectionManagerBuilder.create()
                .setDefaultConnectionConfig(connection)
                .setMaxConnTotal(MAX_CONNECTIONS)
                .setMaxConnPerRoute(MAX_CONNECTIONS)
                .build())
            .setDefaultRequestConfig(request)
            .disableAutomaticRetries()
            .disableCookieManagement()
            .build();
    }

    @Override
    public Page fetch(TargetUrl url) throws FetchException {
        try {
            return client.execute(new HttpGet(url.uri()), this::page);
        } catch (TooLarge e) {
            throw new FetchException(FetchException.Reason.TOO_LARGE, e.getMessage());
        } catch (IOException e) {
            log.debug("Fetch failed: url={}", url.uri(), e);
            throw new FetchException(FetchException.Reason.TARGET, e.getClass().getSimpleName() + ": " + e.getMessage(), e);
        }
    }

    @Override
    public void close() throws IOException {
        client.close();
    }

    private Page page(ClassicHttpResponse response) throws IOException {
        final List<Page.Header> headers = new ArrayList<>();
        for (Header h : response.getHeaders()) {
            headers.add(new Page.Header(h.getName(), h.getValue()));
        }
        return new Page(response.getCode(), headers, read(response.getEntity()));
    }

    private byte[] read(HttpEntity entity) throws IOException {
        if (entity == null) {
            return new byte[0];
        }
        if (entity.getContentLength() > maxBody) {
            throw new TooLarge("body of " + entity.getContentLength() + " bytes is larger than the limit of " + maxBody);
        }
        try (InputStream in = entity.getContent()) {
            final byte[] body = in.readNBytes(maxBody + 1);
            if (body.length > maxBody) {
                throw new TooLarge("body is larger than the limit of " + maxBody + " bytes");
            }
            return body;
        }
    }

    /**
     * Thrown from the response handler, which may throw only I/O exceptions; not a failure of the site.
     */
    private static final class TooLarge extends IOException {
        TooLarge(String message) {
            super(message);
        }
    }
}
