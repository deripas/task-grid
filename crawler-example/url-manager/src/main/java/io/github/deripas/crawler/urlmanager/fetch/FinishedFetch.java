package io.github.deripas.crawler.urlmanager.fetch;

import java.io.IOException;
import java.util.Map;

/**
 * A fetch whose task has finished.
 *
 * @param id   id given on submit
 * @param tags tags given on submit
 */
public record FinishedFetch(
    String id,
    Map<String, String> tags,
    Outcome outcome
) {
    /**
     * Reads the outcome: a page may be read from blob storage, so it is read on demand, on the thread of the receiver.
     */
    @FunctionalInterface
    public interface Outcome {
        FetchOutcome read() throws IOException;
    }

    public FinishedFetch {
        tags = Map.copyOf(tags);
    }

    /**
     * Reads the page or the failure; the page must be closed.
     */
    public FetchOutcome read() throws IOException {
        return outcome.read();
    }
}
