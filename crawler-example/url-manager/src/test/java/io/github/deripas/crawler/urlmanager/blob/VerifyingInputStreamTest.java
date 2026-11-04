package io.github.deripas.crawler.urlmanager.blob;

import com.github.luben.zstd.Zstd;
import io.github.deripas.taskgrid.task.Blob;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Decoding and checking of stored results; reading from S3 itself - UrlManagerIT.
 */
class VerifyingInputStreamTest {
    private static final byte[] CONTENT = "<urlset><url><loc>https://example.com/</loc></url></urlset>".repeat(1000)
        .getBytes(StandardCharsets.UTF_8);
    private static final Blob.Ref REF = new Blob.Ref("s3://results/t1/x", CONTENT.length, Blob.of(CONTENT).hash(), "zstd");

    @Test
    void zstdContentIsDecodedAndVerified() throws IOException {
        final InputStream in = verified(Zstd.compress(CONTENT), REF);

        assertArrayEquals(CONTENT, in.readAllBytes());
    }

    @Test
    void contentStoredAsIs() throws IOException {
        final Blob.Ref plain = new Blob.Ref(REF.location(), REF.size(), REF.hash(), null);

        assertArrayEquals(CONTENT, verified(CONTENT, plain).readAllBytes());
    }

    @Test
    void otherContentFails() throws IOException {
        final byte[] other = CONTENT.clone();
        other[10] = 'X';
        final InputStream in = verified(Zstd.compress(other), REF);

        final IOException e = assertThrows(IOException.class, in::readAllBytes);
        assertTrue(e.getMessage().contains("sha256"), e.getMessage());
    }

    @Test
    void shorterContentFails() throws IOException {
        final byte[] shorter = new byte[CONTENT.length - 1];
        System.arraycopy(CONTENT, 0, shorter, 0, shorter.length);
        final InputStream in = verified(Zstd.compress(shorter), REF);

        final IOException e = assertThrows(IOException.class, in::readAllBytes);
        assertTrue(e.getMessage().contains("bytes"), e.getMessage());
    }

    @Test
    void unknownEncodingFails() {
        final Blob.Ref gzip = new Blob.Ref(REF.location(), REF.size(), REF.hash(), "gzip");

        assertThrows(IOException.class, () -> S3BlobReader.decoded(new ByteArrayInputStream(CONTENT), gzip));
    }

    @Test
    void locationIsBucketAndKey() throws IOException {
        assertArrayEquals(new String[] {"results", "it/t1/abc"}, S3BlobReader.location("s3://results/it/t1/abc"));
        assertThrows(IOException.class, () -> S3BlobReader.location("https://results/it/t1/abc"));
        assertThrows(IOException.class, () -> S3BlobReader.location("s3://results"));
        assertThrows(IOException.class, () -> S3BlobReader.location("s3://results/"));
        assertThrows(IOException.class, () -> S3BlobReader.location("s3:///key"));
    }

    private static InputStream verified(byte[] stored, Blob.Ref ref) throws IOException {
        return new VerifyingInputStream(S3BlobReader.decoded(new ByteArrayInputStream(stored), ref), ref.size(), ref.hash());
    }
}
