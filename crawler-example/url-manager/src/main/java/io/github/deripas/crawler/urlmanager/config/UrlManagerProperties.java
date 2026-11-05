package io.github.deripas.crawler.urlmanager.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.time.Duration;

/**
 * Settings {@code url-manager.*}; defaults - {@code application.yaml}.
 *
 * @param grpcPort        port of the node for sidecars; 0 - any free one
 * @param defaultTimeout  wait for a page when the client gives none
 * @param maxTimeout      upper bound of the timeout of a client
 * @param maxAttempts     attempts per fetch, including the first one
 * @param crawlDelay      pause of a host after a result before its next fetch
 * @param resultTtl       how long a result nobody waits for is kept
 * @param shutdownTimeout wait for the gRPC server to terminate
 */
@ConfigurationProperties("url-manager")
public record UrlManagerProperties(
    int grpcPort,
    Duration defaultTimeout,
    Duration maxTimeout,
    int maxAttempts,
    Duration crawlDelay,
    Duration resultTtl,
    Duration shutdownTimeout,
    Blob blob
) {
    /**
     * S3 storage of large results, the one the sidecars of the fetcher write to.
     *
     * @param endpoint  e.g. {@code http://seaweedfs:8333}; empty - no blob storage, large results cannot be read
     * @param accessKey empty - the default credentials of the AWS SDK (environment, profile, role)
     */
    public record Blob(
        String endpoint,
        String region,
        String accessKey,
        String secretKey
    ) {
        public boolean configured() {
            return endpoint != null && !endpoint.isBlank();
        }
    }

    public UrlManagerProperties {
        if (defaultTimeout.compareTo(maxTimeout) > 0) {
            throw new IllegalArgumentException("default-timeout " + defaultTimeout + " above max-timeout " + maxTimeout);
        }
    }
}
