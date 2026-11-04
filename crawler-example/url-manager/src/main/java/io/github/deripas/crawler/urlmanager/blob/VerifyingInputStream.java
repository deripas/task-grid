package io.github.deripas.crawler.urlmanager.blob;

import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;

/**
 * Counts and hashes what is read; at the end of the stream fails if size or SHA-256 differ from the expected ones.
 * The content is sent on as it is read, so a mismatch breaks the response instead of preventing it.
 */
final class VerifyingInputStream extends FilterInputStream {
    private final long size;
    private final String sha256;
    private final MessageDigest digest;
    private long read;
    private boolean verified;

    VerifyingInputStream(
        InputStream in,
        long size,
        String sha256
    ) {
        super(in);
        this.size = size;
        this.sha256 = sha256;
        try {
            this.digest = MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 is required of every JDK", e);
        }
    }

    @Override
    public int read() throws IOException {
        final byte[] one = new byte[1];
        return read(one, 0, 1) < 0
            ? -1
            : one[0] & 0xff;
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        final int n = in.read(b, off, len);
        if (n < 0) {
            verify();
            return -1;
        }
        digest.update(b, off, n);
        read += n;
        if (read > size) {
            throw new IOException("blob is larger than " + size + " bytes");
        }
        return n;
    }

    /**
     * Skipped bytes would not be hashed: read them.
     */
    @Override
    public long skip(long n) throws IOException {
        final byte[] buffer = new byte[8192];
        long skipped = 0;
        while (skipped < n) {
            final int r = read(buffer, 0, (int) Math.min(buffer.length, n - skipped));
            if (r < 0) {
                break;
            }
            skipped += r;
        }
        return skipped;
    }

    @Override
    public boolean markSupported() {
        return false;
    }

    private void verify() throws IOException {
        if (verified) {
            return;
        }
        verified = true;
        if (read != size) {
            throw new IOException("blob has " + read + " bytes, expected " + size);
        }
        final String actual = HexFormat.of().formatHex(digest.digest());
        if (!actual.equals(sha256)) {
            throw new IOException("blob sha256 " + actual + ", expected " + sha256);
        }
    }
}
