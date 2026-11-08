package io.github.deripas.crawler.urlmanager.fetch;

/**
 * How a fetch ended for the client.
 */
public sealed interface FetchOutcome {
    /**
     * The site answered; the page must be closed.
     */
    record Fetched(Page page) implements FetchOutcome {
    }

    /**
     * Final failure: the site cannot be reached, the body is too large, attempts are exhausted.
     */
    record Failed(String error) implements FetchOutcome {
    }

    /**
     * No result within the timeout of the client; the task may still finish.
     */
    record TimedOut() implements FetchOutcome {
    }

    /**
     * Not submitted: the client has as many requests in work on this node as its box allows (until their results
     * are taken, also of those timed out).
     */
    record Busy() implements FetchOutcome {
    }
}
