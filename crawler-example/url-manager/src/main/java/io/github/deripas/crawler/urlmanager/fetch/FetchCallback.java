package io.github.deripas.crawler.urlmanager.fetch;

import java.util.concurrent.CompletionStage;

/**
 * Receives finished fetches on the node where they finished (see {@link FetchService#onFinished}).
 * <p>
 * Called on a thread of the queue: must not block - reading the page ({@link FinishedFetch#read()}) may go to
 * blob storage, so hand it over to an executor and complete the stage when the fetch is handled. An in-memory queue
 * delivers once; a persistent one again until the stage completes normally.
 */
public interface FetchCallback {
    CompletionStage<Void> finished(FinishedFetch fetch);
}
