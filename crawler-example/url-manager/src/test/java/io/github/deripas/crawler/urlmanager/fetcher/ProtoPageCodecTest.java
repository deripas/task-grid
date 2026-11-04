package io.github.deripas.crawler.urlmanager.fetcher;

import com.google.protobuf.ByteString;
import com.google.protobuf.CodedOutputStream;
import io.github.deripas.crawler.fetcher.v1.FetchRequest;
import io.github.deripas.crawler.fetcher.v1.FetchResponse;
import io.github.deripas.crawler.fetcher.v1.Header;
import io.github.deripas.crawler.urlmanager.fetch.Page;
import io.github.deripas.crawler.urlmanager.fetch.PageUrl;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ProtoPageCodecTest {
    private final ProtoPageCodec codec = new ProtoPageCodec();

    @Test
    void requestCarriesTheUrl() throws IOException {
        final byte[] request = codec.request(PageUrl.parse("https://example.com/a"));

        assertEquals("https://example.com/a", FetchRequest.parseFrom(request).getUrl());
    }

    @Test
    void pageIsReadFromTheResponse() throws IOException {
        final byte[] body = new byte[3 << 20];
        Arrays.fill(body, (byte) 'x');
        final FetchResponse response = FetchResponse.newBuilder()
            .setStatus(404)
            .addHeaders(Header.newBuilder().setName("Content-Type").setValue("text/html"))
            .addHeaders(Header.newBuilder().setName("Set-Cookie").setValue("a=1"))
            .setBody(ByteString.copyFrom(body))
            .build();

        try (Page page = codec.page(new ByteArrayInputStream(response.toByteArray()))) {
            assertEquals(404, page.status());
            assertEquals(List.of(new Page.Header("Content-Type", "text/html"), new Page.Header("Set-Cookie", "a=1")), page.headers());
            assertEquals("text/html", page.header("content-type").orElseThrow());
            assertEquals(body.length, page.bodySize());
            assertArrayEquals(body, page.body().readAllBytes());
        }
    }

    /**
     * The body is not read ahead: the stream underneath is consumed as the body is, and read to its end with it.
     */
    @Test
    void bodyIsStreamedAndTheStreamIsReadToItsEnd() throws IOException {
        final byte[] wire = FetchResponse.newBuilder().setStatus(200).setBody(ByteString.copyFromUtf8("0123456789")).build().toByteArray();
        final AtomicBoolean ended = new AtomicBoolean();
        final InputStream stream = new ByteArrayInputStream(wire) {
            @Override
            public synchronized int read(byte[] b, int off, int len) {
                final int n = super.read(b, off, len);
                ended.compareAndSet(false, n < 0);
                return n;
            }
        };

        final Page page = codec.page(stream);
        final byte[] first = page.body().readNBytes(4);
        assertEquals("0123", new String(first));
        assertEquals(6, stream.available());
        assertEquals("456789", new String(page.body().readAllBytes()));
        assertTrue(ended.get());
    }

    @Test
    void emptyResponseIsAnEmptyPage() throws IOException {
        final Page page = codec.page(new ByteArrayInputStream(new byte[0]));

        assertEquals(0, page.status());
        assertEquals(0, page.bodySize());
        assertEquals(-1, page.body().read());
    }

    @Test
    void unknownFieldsAreSkipped() throws IOException {
        final ByteArrayOutputStream wire = new ByteArrayOutputStream();
        final CodedOutputStream out = CodedOutputStream.newInstance(wire);
        out.writeInt32(FetchResponse.STATUS_FIELD_NUMBER, 200);
        out.writeString(10, "future field");
        out.writeFixed64(11, 42);
        out.writeFixed32(12, 7);
        out.writeInt64(13, -1);
        out.writeBytes(FetchResponse.BODY_FIELD_NUMBER, ByteString.copyFromUtf8("body"));
        out.flush();

        final Page page = codec.page(new ByteArrayInputStream(wire.toByteArray()));

        assertEquals(200, page.status());
        assertEquals("body", new String(page.body().readAllBytes()));
    }

    @Test
    void truncatedBodyFails() throws IOException {
        final byte[] wire = FetchResponse.newBuilder().setBody(ByteString.copyFromUtf8("0123456789")).build().toByteArray();

        final Page page = codec.page(new ByteArrayInputStream(Arrays.copyOf(wire, wire.length - 3)));

        assertThrows(EOFException.class, () -> page.body().readAllBytes());
    }
}
