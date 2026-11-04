package io.github.deripas.crawler.urlmanager.fetch;

import io.github.deripas.taskgrid.task.Blob;

import java.io.IOException;
import java.io.InputStream;

/**
 * Port: content of a result in blob storage, decoded; the stream fails if the content does not match the reference.
 */
public interface BlobReader {
    InputStream open(Blob.Ref ref) throws IOException;
}
