package io.github.deripas.crawler.fetcher.config;

import java.time.Duration;
import java.time.format.DateTimeParseException;
import java.util.Locale;
import java.util.Map;

/**
 * Settings of the fetcher from the environment.
 *
 * @param port            gRPC port (PORT, 50051); the sidecar calls it on localhost
 * @param connectTimeout  CONNECT_TIMEOUT, 10s
 * @param responseTimeout RESPONSE_TIMEOUT, 30s: limit of waiting for data of the response
 * @param maxBody         MAX_BODY, 100MiB: limit of the decoded body
 * @param shutdownTimeout SHUTDOWN_TIMEOUT, 60s: on SIGTERM calls in progress are finished within it
 */
public record FetcherSettings(
    int port,
    Duration connectTimeout,
    Duration responseTimeout,
    int maxBody,
    Duration shutdownTimeout
) {
    public FetcherSettings {
        if (port < 0 || port > 65535) {
            throw new IllegalArgumentException("PORT: " + port + ", want 0..65535");
        }
        positive(connectTimeout, "CONNECT_TIMEOUT");
        positive(responseTimeout, "RESPONSE_TIMEOUT");
        positive(shutdownTimeout, "SHUTDOWN_TIMEOUT");
        if (maxBody < 1) {
            throw new IllegalArgumentException("MAX_BODY: " + maxBody + ", want > 0");
        }
    }

    /**
     * Durations as {@code 10s}, {@code 500ms}, {@code 1m}; sizes as bytes or with {@code KiB}, {@code MiB}.
     *
     * @throws IllegalArgumentException an invalid value, named by its variable
     */
    public static FetcherSettings from(Map<String, String> env) {
        return new FetcherSettings(
            integer(env, "PORT", "50051"),
            duration(env, "CONNECT_TIMEOUT", "10s"),
            duration(env, "RESPONSE_TIMEOUT", "30s"),
            size(env, "MAX_BODY", "100MiB"),
            duration(env, "SHUTDOWN_TIMEOUT", "60s"));
    }

    private static int integer(
        Map<String, String> env,
        String name,
        String def
    ) {
        final String value = value(env, name, def);
        try {
            return Integer.parseInt(value);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException(name + ": " + value + ", want a number");
        }
    }

    private static Duration duration(
        Map<String, String> env,
        String name,
        String def
    ) {
        final String value = value(env, name, def);
        try {
            return value.endsWith("ms")
                ? Duration.ofMillis(Long.parseLong(value.substring(0, value.length() - 2)))
                : Duration.parse("PT" + value.toUpperCase(Locale.ROOT));
        } catch (NumberFormatException | DateTimeParseException e) {
            throw new IllegalArgumentException(name + ": " + value + ", want e.g. 10s, 500ms, 1m");
        }
    }

    private static int size(
        Map<String, String> env,
        String name,
        String def
    ) {
        final String value = value(env, name, def);
        final long multiplier = value.endsWith("MiB")
            ? 1L << 20
            : value.endsWith("KiB")
                ? 1L << 10
                : 1;
        final String digits = multiplier == 1
            ? value
            : value.substring(0, value.length() - 3);
        try {
            final long bytes = Long.parseLong(digits.trim()) * multiplier;
            if (bytes > Integer.MAX_VALUE - 8) {
                throw new IllegalArgumentException(name + ": " + value + ", want at most 2GiB");
            }
            return (int) bytes;
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException(name + ": " + value + ", want bytes, KiB or MiB");
        }
    }

    private static String value(
        Map<String, String> env,
        String name,
        String def
    ) {
        final String value = env.get(name);
        return value == null || value.isBlank()
            ? def
            : value.trim();
    }

    private static void positive(Duration value, String name) {
        if (value == null || !value.isPositive()) {
            throw new IllegalArgumentException(name + ": " + value + ", want > 0");
        }
    }
}
