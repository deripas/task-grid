package io.github.deripas.crawler.urlmanager.fetcher;

import io.github.deripas.crawler.fetcher.v1.FetchRequest;
import io.github.deripas.crawler.fetcher.v1.FetchResponse;
import io.github.deripas.crawler.fetcher.v1.Header;
import io.github.deripas.crawler.urlmanager.fetch.Page;
import io.github.deripas.crawler.urlmanager.fetch.PageCodec;
import io.github.deripas.crawler.urlmanager.fetch.PageUrl;

import java.io.EOFException;
import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

/**
 * Protocol {@code crawler.fetcher.v1}: the payload is {@link FetchRequest}, the result of the handler is
 * {@link FetchResponse}.
 * <p>
 * The response is read field by field from the stream instead of {@code FetchResponse.parseFrom}: status and headers
 * are parsed, the body is handed over as the rest of the stream, so a body of 100 MiB never sits in memory.
 * This relies on the body being the last field on the wire: protobuf serializers write fields in the order of their
 * numbers, and {@code body} has the highest. Fields after it are skipped.
 */
public final class ProtoPageCodec implements PageCodec {
    private static final int WIRE_VARINT = 0;
    private static final int WIRE_FIXED64 = 1;
    private static final int WIRE_LEN = 2;
    private static final int WIRE_FIXED32 = 5;

    @Override
    public byte[] request(PageUrl url) {
        return FetchRequest.newBuilder().setUrl(url.uri().toString()).build().toByteArray();
    }

    @Override
    public Page page(InputStream result) throws IOException {
        int status = 0;
        final List<Page.Header> headers = new ArrayList<>();
        while (true) {
            final long tag = varint(result, true);
            if (tag < 0) {
                return new Page(status, headers, 0, result);
            }
            final int field = (int) (tag >>> 3);
            final int wire = (int) (tag & 7);
            if (field == FetchResponse.STATUS_FIELD_NUMBER && wire == WIRE_VARINT) {
                status = (int) varint(result, false);
            } else if (field == FetchResponse.HEADERS_FIELD_NUMBER && wire == WIRE_LEN) {
                final Header header = Header.parseFrom(result.readNBytes(length(result)));
                headers.add(new Page.Header(header.getName(), header.getValue()));
            } else if (field == FetchResponse.BODY_FIELD_NUMBER && wire == WIRE_LEN) {
                final long size = length(result);
                return new Page(status, headers, size, new Body(result, size));
            } else {
                skip(result, wire);
            }
        }
    }

    private static void skip(InputStream in, int wire) throws IOException {
        switch (wire) {
            case WIRE_VARINT -> varint(in, false);
            case WIRE_FIXED64 -> in.skipNBytes(8);
            case WIRE_LEN -> in.skipNBytes(length(in));
            case WIRE_FIXED32 -> in.skipNBytes(4);
            default -> throw new IOException("unsupported wire type " + wire);
        }
    }

    private static int length(InputStream in) throws IOException {
        final long length = varint(in, false);
        if (length < 0 || length > Integer.MAX_VALUE) {
            throw new IOException("bad length " + length);
        }
        return (int) length;
    }

    /**
     * Base-128 varint; -1 at the end of the stream before its first byte when {@code eofAllowed}.
     */
    private static long varint(InputStream in, boolean eofAllowed) throws IOException {
        long value = 0;
        for (int shift = 0; shift < 64; shift += 7) {
            final int b = in.read();
            if (b < 0) {
                if (eofAllowed && shift == 0) {
                    return -1;
                }
                throw new EOFException("truncated varint");
            }
            value |= (long) (b & 0x7f) << shift;
            if ((b & 0x80) == 0) {
                return value;
            }
        }
        throw new IOException("malformed varint");
    }

    /**
     * The body: exactly {@code size} bytes of the stream. At its end the rest of the stream is read out, so that
     * a verifying stream underneath (size, hash of a blob) sees the end of the content.
     */
    private static final class Body extends FilterInputStream {
        private long remaining;

        Body(InputStream in, long size) throws IOException {
            super(in);
            this.remaining = size;
            if (size == 0) {
                drain();
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
            if (remaining == 0) {
                return -1;
            }
            final int n = in.read(b, off, (int) Math.min(len, remaining));
            if (n < 0) {
                throw new EOFException("body ended " + remaining + " bytes early");
            }
            remaining -= n;
            if (remaining == 0) {
                drain();
            }
            return n;
        }

        @Override
        public long skip(long n) throws IOException {
            final long skipped = in.skip(Math.min(n, remaining));
            remaining -= skipped;
            if (remaining == 0 && skipped > 0) {
                drain();
            }
            return skipped;
        }

        @Override
        public int available() throws IOException {
            return (int) Math.min(in.available(), remaining);
        }

        @Override
        public boolean markSupported() {
            return false;
        }

        /**
         * Reads to the end with read() itself: transferTo of some streams skips it, and so their checks.
         */
        private void drain() throws IOException {
            final byte[] rest = new byte[512];
            while (in.read(rest, 0, rest.length) >= 0) {
                // fields after the body are not needed
            }
        }
    }
}
