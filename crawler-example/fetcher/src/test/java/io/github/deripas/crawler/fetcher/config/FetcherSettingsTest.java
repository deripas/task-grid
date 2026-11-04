package io.github.deripas.crawler.fetcher.config;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class FetcherSettingsTest {
    @Test
    void defaults() {
        assertEquals(new FetcherSettings(50051, Duration.ofSeconds(10), Duration.ofSeconds(30), 100 << 20, Duration.ofSeconds(60)),
            FetcherSettings.from(Map.of()));
    }

    @Test
    void valuesFromTheEnvironment() {
        final FetcherSettings s = FetcherSettings.from(Map.of(
            "PORT", "6000",
            "CONNECT_TIMEOUT", "500ms",
            "RESPONSE_TIMEOUT", "1m30s",
            "MAX_BODY", "512KiB",
            "SHUTDOWN_TIMEOUT", "5s"));
        assertEquals(new FetcherSettings(6000, Duration.ofMillis(500), Duration.ofSeconds(90), 512 << 10, Duration.ofSeconds(5)), s);
    }

    @Test
    void invalidValuesNameTheirVariable() {
        final Map<String, String> invalid = Map.of(
            "PORT", "x",
            "CONNECT_TIMEOUT", "0s",
            "RESPONSE_TIMEOUT", "soon",
            "MAX_BODY", "4MB",
            "SHUTDOWN_TIMEOUT", "-1s");
        invalid.forEach((name, value) -> {
            final IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
                () -> FetcherSettings.from(Map.of(name, value)));
            assertTrue(e.getMessage().startsWith(name), e.getMessage());
        });
        assertThrows(IllegalArgumentException.class, () -> FetcherSettings.from(Map.of("MAX_BODY", "4096MiB")));
    }
}
