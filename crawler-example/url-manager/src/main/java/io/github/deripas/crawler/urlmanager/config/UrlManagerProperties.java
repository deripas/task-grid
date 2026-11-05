package io.github.deripas.crawler.urlmanager.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.time.Duration;

/**
 * Settings {@code url-manager.*}; defaults - {@code application.yaml}.
 *
 * @param grpcPort        port of the node for sidecars; 0 - any free one
 * @param grpcKeepalive   HTTP/2 pings of the connections of sidecars
 * @param defaultTimeout  wait for a page when the client gives none
 * @param maxTimeout      upper bound of the timeout of a client
 * @param maxAttempts     attempts per fetch, including the first one
 * @param crawlDelay      pause of a host after a result before its next fetch
 * @param resultTtl       how long a result nobody waits for is kept
 * @param shutdownTimeout wait for the gRPC server to terminate
 * @param rabbit          requests over RabbitMQ
 */
@ConfigurationProperties("url-manager")
public record UrlManagerProperties(
    int grpcPort,
    GrpcKeepalive grpcKeepalive,
    Duration defaultTimeout,
    Duration maxTimeout,
    int maxAttempts,
    Duration crawlDelay,
    Duration resultTtl,
    Duration shutdownTimeout,
    Blob blob,
    Rabbit rabbit
) {
    /**
     * HTTP/2 pings of the connections of sidecars: a hung connection (open, nothing passes) is closed.
     *
     * @param time       ping a connection quiet this long; gRPC raises it to 10s at least
     * @param timeout    no answer to a ping this long - the connection is closed, its session ends
     * @param permitTime sidecars may ping this often; at most their {@code NODE_KEEPALIVE_TIME}
     */
    public record GrpcKeepalive(
        Duration time,
        Duration timeout,
        Duration permitTime
    ) {
    }

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

    /**
     * Request queue of RabbitMQ; the connection, prefetch and publisher confirms are {@code spring.rabbitmq.*}.
     *
     * @param enabled consume the queue; off - only {@code POST /fetch}
     * @param queue   durable queue of requests, declared on start
     * @param ack     when a request is acknowledged
     */
    public record Rabbit(
        boolean enabled,
        String queue,
        Ack ack
    ) {
    }

    /**
     * When a request is acknowledged - which way of handling the queue gets.
     */
    public enum Ack {
        /**
         * After its reply is confirmed by the broker; the request waits for its page on the url-manager that took it.
         * For the in-memory queue of tasks, until it is gone: a request of a stopped url-manager stays in RabbitMQ.
         * The requests of a host must come to one url-manager (consistent-hash exchange by host).
         */
        AFTER_REPLY,

        /**
         * As soon as its task is queued; the reply is sent by the result callback of the queue, wherever the task
         * finishes. The target way, for a queue that keeps its tasks; with the in-memory one a stop loses requests.
         */
        AFTER_SUBMIT
    }

    public UrlManagerProperties {
        if (defaultTimeout.compareTo(maxTimeout) > 0) {
            throw new IllegalArgumentException("default-timeout " + defaultTimeout + " above max-timeout " + maxTimeout);
        }
    }
}
