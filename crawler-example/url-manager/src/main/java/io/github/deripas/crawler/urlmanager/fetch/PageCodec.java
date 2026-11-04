package io.github.deripas.crawler.urlmanager.fetch;

import java.io.IOException;
import java.io.InputStream;

/**
 * Port: the protocol of the fetcher - payload of a task and the page from the result of the handler.
 */
public interface PageCodec {
    byte[] request(PageUrl url);

    /**
     * Reads the page from the result of the handler; the body is the rest of the stream, not read yet.
     */
    Page page(InputStream result) throws IOException;
}
