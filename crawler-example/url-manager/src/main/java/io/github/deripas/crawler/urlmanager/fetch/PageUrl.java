package io.github.deripas.crawler.urlmanager.fetch;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Locale;

/**
 * Absolute http(s) URL of a page to fetch.
 *
 * @param host lower case: the unit of politeness - fetches of one host run one at a time
 */
public record PageUrl(
    URI uri,
    String host
) {
    public static PageUrl parse(String url) {
        if (url == null || url.isBlank()) {
            throw new InvalidUrlException("url is required");
        }
        final URI uri;
        try {
            uri = new URI(url.strip());
        } catch (URISyntaxException e) {
            throw new InvalidUrlException("not a URL: " + e.getMessage());
        }
        final String scheme = uri.getScheme() == null
            ? ""
            : uri.getScheme().toLowerCase(Locale.ROOT);
        if (!scheme.equals("http") && !scheme.equals("https") || uri.getHost() == null) {
            throw new InvalidUrlException("not an absolute http(s) URL: " + url);
        }
        return new PageUrl(uri, uri.getHost().toLowerCase(Locale.ROOT));
    }
}
