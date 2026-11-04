package io.github.deripas.crawler.urlmanager.blob;

import com.github.luben.zstd.ZstdInputStream;
import io.github.deripas.crawler.urlmanager.fetch.BlobReader;
import io.github.deripas.taskgrid.task.Blob;
import lombok.RequiredArgsConstructor;
import software.amazon.awssdk.core.exception.SdkException;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;

import java.io.IOException;
import java.io.InputStream;

/**
 * Results the handler-sidecar has put into S3: location {@code s3://<bucket>/<key>}, content compressed with zstd
 * or stored as is. The stream checks size and SHA-256 of the decoded content against the reference.
 */
@RequiredArgsConstructor
public final class S3BlobReader implements BlobReader, AutoCloseable {
    private static final String SCHEME = "s3://";

    private final S3Client s3;

    @Override
    public InputStream open(Blob.Ref ref) throws IOException {
        final String[] bucketAndKey = location(ref.location());
        final InputStream stored;
        try {
            stored = s3.getObject(GetObjectRequest.builder().bucket(bucketAndKey[0]).key(bucketAndKey[1]).build());
        } catch (SdkException e) {
            throw new IOException("blob " + ref.location() + ": " + e.getMessage(), e);
        }
        return new VerifyingInputStream(decoded(stored, ref), ref.size(), ref.hash());
    }

    @Override
    public void close() {
        s3.close();
    }

    /**
     * {@code s3://bucket/key} -> bucket, key.
     */
    static String[] location(String location) throws IOException {
        final int slash = location.indexOf('/', SCHEME.length());
        if (!location.startsWith(SCHEME) || slash <= SCHEME.length() || slash == location.length() - 1) {
            throw new IOException("not an S3 location: " + location);
        }
        return new String[] {location.substring(SCHEME.length(), slash), location.substring(slash + 1)};
    }

    static InputStream decoded(InputStream stored, Blob.Ref ref) throws IOException {
        if (ref.encoding() == null) {
            return stored;
        }
        if (ref.encoding().equals("zstd")) {
            return new ZstdInputStream(stored);
        }
        stored.close();
        throw new IOException("unknown encoding " + ref.encoding() + " of " + ref.location());
    }
}
