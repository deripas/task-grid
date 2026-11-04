package io.github.deripas.crawler.fetcher.fetch;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Locale;

/**
 * URL to fetch: absolute, http or https, with a host.
 */
public record TargetUrl(URI uri) {
    /**
     * @throws FetchException {@link FetchException.Reason#INVALID_URL} - not an absolute http(s) URL
     */
    public static TargetUrl parse(String url) throws FetchException {
        final URI uri;
        try {
            uri = new URI(url);
        } catch (URISyntaxException e) {
            throw new FetchException(FetchException.Reason.INVALID_URL, "invalid URL: " + e.getMessage());
        }
        final String scheme = uri.getScheme() == null
            ? ""
            : uri.getScheme().toLowerCase(Locale.ROOT);
        if (!scheme.equals("http") && !scheme.equals("https") || uri.getHost() == null) {
            throw new FetchException(FetchException.Reason.INVALID_URL, "not an absolute http(s) URL: " + url);
        }
        return new TargetUrl(uri);
    }
}
