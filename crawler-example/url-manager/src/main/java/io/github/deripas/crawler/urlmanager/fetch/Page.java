package io.github.deripas.crawler.urlmanager.fetch;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * Response of the site. The body is a stream, not bytes: a large one (a sitemap of 100 MiB) is read from blob
 * storage as it is sent to the client. The page must be closed.
 *
 * @param status   HTTP status of the site; 404 and 500 are pages too
 * @param bodySize bytes of the body; -1 - not known
 */
public record Page(
    int status,
    List<Header> headers,
    long bodySize,
    InputStream body
) implements AutoCloseable {
    public record Header(String name, String value) {
    }

    public Page {
        headers = List.copyOf(headers);
        Objects.requireNonNull(body, "body");
    }

    /**
     * The first header of that name, case-insensitive.
     */
    public Optional<String> header(String name) {
        return headers.stream().filter(h -> h.name().equalsIgnoreCase(name)).map(Header::value).findFirst();
    }

    @Override
    public void close() throws IOException {
        body.close();
    }
}
