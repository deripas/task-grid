package io.github.deripas.crawler.urlmanager.api;

/**
 * Body of {@code POST /fetch}.
 *
 * @param url       absolute http(s) URL
 * @param timeoutMs how long to wait for the page; absent - the default of the service, capped by its maximum
 */
public record FetchCommand(
    String url,
    Long timeoutMs
) {
}
