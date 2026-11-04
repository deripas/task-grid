package io.github.deripas.crawler.fetcher.fetch;

import java.util.List;
import java.util.Objects;

/**
 * Response of the site.
 *
 * @param status  HTTP status of the final response, after redirects
 * @param headers headers of the final response, in their order
 * @param body    body decoded from Content-Encoding; the array is not copied
 */
public record Page(
    int status,
    List<Header> headers,
    byte[] body
) {
    /**
     * A response header.
     */
    public record Header(String name, String value) {
    }

    public Page {
        headers = List.copyOf(headers);
        Objects.requireNonNull(body, "body");
    }

    @Override
    public String toString() {
        return "Page[status=" + status + ", headers=" + headers.size() + ", body=" + body.length + "]";
    }
}
