package io.github.deripas.crawler.urlmanager.fetch;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.NullAndEmptySource;
import org.junit.jupiter.params.provider.ValueSource;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class PageUrlTest {
    @Test
    void hostIsLowerCase() {
        final PageUrl url = PageUrl.parse(" HTTPS://Example.COM:8443/a?b=c ");

        assertEquals("example.com", url.host());
        assertEquals("HTTPS://Example.COM:8443/a?b=c", url.uri().toString());
    }

    @ParameterizedTest
    @NullAndEmptySource
    @ValueSource(strings = {"example.com/a", "ftp://example.com/a", "http://", "http://exa mple.com/", "mailto:a@example.com"})
    void notAnAbsoluteHttpUrlIsInvalid(String url) {
        assertThrows(InvalidUrlException.class, () -> PageUrl.parse(url));
    }
}
