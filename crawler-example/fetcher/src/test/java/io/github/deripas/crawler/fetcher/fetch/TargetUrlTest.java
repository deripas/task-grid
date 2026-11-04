package io.github.deripas.crawler.fetcher.fetch;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class TargetUrlTest {
    @Test
    void absoluteHttpUrlsAreAccepted() throws FetchException {
        assertEquals("https://example.com/a?b=1", TargetUrl.parse("https://example.com/a?b=1").uri().toString());
        assertEquals("HTTP://example.com", TargetUrl.parse("HTTP://example.com").uri().toString());
    }

    @ParameterizedTest
    @ValueSource(strings = {"", "example.com", "/path", "ftp://example.com/file", "http://", "http://exa mple.com", "mailto:a@b.c"})
    void otherUrlsAreInvalid(String url) {
        final FetchException e = assertThrows(FetchException.class, () -> TargetUrl.parse(url));
        assertEquals(FetchException.Reason.INVALID_URL, e.getReason());
    }
}
