package io.github.deripas.crawler.fetcher.fetch;

import lombok.Getter;

/**
 * The fetch did not give a response of the site. Any response of the site, including 404 and 500, is a {@link Page}.
 */
@Getter
public final class FetchException extends Exception {
    /**
     * Why there is no page.
     */
    public enum Reason {
        /**
         * Not an absolute http(s) URL.
         */
        INVALID_URL,

        /**
         * The site cannot be reached: DNS, connect, TLS, timeout, broken response.
         */
        TARGET,

        /**
         * The body is larger than the limit of the fetcher.
         */
        TOO_LARGE
    }

    private final Reason reason;

    public FetchException(Reason reason, String message) {
        super(message);
        this.reason = reason;
    }

    public FetchException(
        Reason reason,
        String message,
        Throwable cause
    ) {
        super(message, cause);
        this.reason = reason;
    }
}
