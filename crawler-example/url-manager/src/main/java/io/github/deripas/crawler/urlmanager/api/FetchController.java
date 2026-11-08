package io.github.deripas.crawler.urlmanager.api;

import io.github.deripas.crawler.urlmanager.fetch.FetchOutcome;
import io.github.deripas.crawler.urlmanager.fetch.FetchService;
import io.github.deripas.crawler.urlmanager.fetch.InvalidUrlException;
import io.github.deripas.crawler.urlmanager.fetch.Page;
import io.github.deripas.crawler.urlmanager.fetch.PageUrl;
import jakarta.servlet.http.HttpServletResponse;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ProblemDetail;
import org.springframework.web.ErrorResponseException;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

import java.io.IOException;
import java.io.OutputStream;
import java.time.Duration;

/**
 * {@code POST /fetch {"url": ..., "timeoutMs": ...}} - fetches the page and answers like a proxy:
 * <ul>
 *   <li>the site answered - 200, the body of the page with its {@code Content-Type}, the status of the site
 *   in {@value #FETCH_STATUS} (404 of the site is a page too);</li>
 *   <li>the fetch failed (site unreachable, body too large, attempts exhausted) - 502, problem detail;</li>
 *   <li>no result within the timeout - 504;</li>
 *   <li>too many requests in work on this url-manager (its box in the mailbox is full) - 429;</li>
 *   <li>not an http(s) URL, bad timeout - 400.</li>
 * </ul>
 * The body is copied from its stream (blob storage for large pages) as it is read.
 */
@RestController
@RequiredArgsConstructor
public final class FetchController {
    public static final String FETCH_STATUS = "X-Fetch-Status";
    /**
     * The client waiting on this node: its box in the mailbox.
     */
    static final String CLIENT = "rest";

    private final FetchService service;
    private final Duration defaultTimeout;
    private final Duration maxTimeout;

    @PostMapping(path = "/fetch", consumes = MediaType.APPLICATION_JSON_VALUE)
    public void fetch(@RequestBody FetchCommand command, HttpServletResponse response) throws IOException {
        final PageUrl url;
        try {
            url = PageUrl.parse(command.url());
        } catch (InvalidUrlException e) {
            throw problem(HttpStatus.BAD_REQUEST, e.getMessage());
        }
        switch (service.fetch(CLIENT, url, timeout(command))) {
            case FetchOutcome.Fetched fetched -> send(fetched.page(), response);
            case FetchOutcome.Failed failed -> throw problem(HttpStatus.BAD_GATEWAY, failed.error());
            case FetchOutcome.TimedOut ignored -> throw problem(HttpStatus.GATEWAY_TIMEOUT, "no page within the timeout");
            case FetchOutcome.Busy ignored -> throw problem(HttpStatus.TOO_MANY_REQUESTS, "too many requests in work");
        }
    }

    private Duration timeout(FetchCommand command) {
        if (command.timeoutMs() == null) {
            return defaultTimeout;
        }
        if (command.timeoutMs() <= 0) {
            throw problem(HttpStatus.BAD_REQUEST, "timeoutMs must be positive");
        }
        final Duration requested = Duration.ofMillis(command.timeoutMs());
        return requested.compareTo(maxTimeout) > 0
            ? maxTimeout
            : requested;
    }

    private static void send(Page page, HttpServletResponse response) throws IOException {
        try (page) {
            response.setStatus(HttpStatus.OK.value());
            response.setHeader(FETCH_STATUS, String.valueOf(page.status()));
            response.setContentType(page.header("Content-Type").orElse(MediaType.APPLICATION_OCTET_STREAM_VALUE));
            if (page.bodySize() >= 0) {
                response.setContentLengthLong(page.bodySize());
            }
            final OutputStream out = response.getOutputStream();
            page.body().transferTo(out);
            out.flush();
        }
    }

    private static ErrorResponseException problem(HttpStatus status, String detail) {
        return new ErrorResponseException(status, ProblemDetail.forStatusAndDetail(status, detail), null);
    }
}
