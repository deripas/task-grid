package io.github.deripas.crawler.urlmanager.fetch;

/**
 * The client gave a URL that cannot be fetched; no task is created.
 */
public final class InvalidUrlException extends RuntimeException {
    public InvalidUrlException(String message) {
        super(message);
    }
}
