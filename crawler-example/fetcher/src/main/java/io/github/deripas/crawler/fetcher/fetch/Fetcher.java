package io.github.deripas.crawler.fetcher.fetch;

/**
 * Port: one HTTP GET of a URL, redirects followed, no retries. Implementation - the http adapter.
 */
public interface Fetcher {
    /**
     * Blocks until the whole body is read.
     *
     * @throws FetchException the site gave no response or the body is too large
     */
    Page fetch(TargetUrl url) throws FetchException;
}
