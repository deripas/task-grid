package io.github.deripas.crawler.urlmanager.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.time.Duration;
import java.util.List;

/**
 * Settings {@code url-manager.*}; defaults - {@code application.yaml}.
 *
 * @param queue           where the tasks live: in this process, or in the cluster of url-managers
 * @param nodeId          this node among the url-managers: the consumer of its boxes in the mailbox (results of
 *                        the clients waiting here)
 * @param cluster         the Pekko cluster and its journal, for {@code queue: cluster}
 * @param grpcPort        port of the node for sidecars; 0 - any free one
 * @param grpcKeepalive   HTTP/2 pings of the connections of sidecars
 * @param defaultTimeout  wait for a page when the client gives none
 * @param maxTimeout      upper bound of the timeout of a client
 * @param maxAttempts     attempts per fetch, including the first one
 * @param attemptTimeout  an attempt without a result this long is retried: the limit for a hung or lost attempt,
 *                        above {@code max-timeout} of a fetch
 * @param crawlDelay      pause of a host after a result before its next fetch
 * @param boxLimit        requests of a client in work at once, per box (client and node): submitted, result not
 *                        taken yet; more - refused as busy
 * @param resultTtl       how long a result nobody takes is kept in the mailbox
 * @param shutdownTimeout wait for the gRPC server to terminate
 * @param rabbit          requests over RabbitMQ
 */
@ConfigurationProperties("url-manager")
public record UrlManagerProperties(
    QueueMode queue,
    String nodeId,
    Cluster cluster,
    int grpcPort,
    GrpcKeepalive grpcKeepalive,
    Duration defaultTimeout,
    Duration maxTimeout,
    int maxAttempts,
    Duration attemptTimeout,
    Duration crawlDelay,
    int boxLimit,
    Duration resultTtl,
    Duration shutdownTimeout,
    Blob blob,
    Rabbit rabbit
) {
    /**
     * Where the tasks live.
     */
    public enum QueueMode {
        /**
         * In this process: lost on a stop; each instance has queues of its own.
         */
        MEMORY,

        /**
         * In the Pekko cluster of url-managers, persisted in ScyllaDB: a host is one partition in the whole cluster,
         * tasks and the boxes of the results survive a stop; a result is awaited on any node.
         */
        CLUSTER
    }

    /**
     * The Pekko cluster of url-managers and its journal.
     *
     * @param host              address other nodes reach this one at (container or pod name)
     * @param port              of Pekko remoting
     * @param seedNodes         {@code host:port} of the nodes to join through; empty - this node alone
     * @param contactPoints     {@code host:port} of ScyllaDB / Cassandra
     * @param datacenter        local datacenter of the driver; keyspaces and tables - {@code cql/schema.cql}
     */
    public record Cluster(
        String host,
        int port,
        List<String> seedNodes,
        List<String> contactPoints,
        String datacenter
    ) {
    }

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
     * Request queue of RabbitMQ; the connection is {@code spring.rabbitmq.*}.
     *
     * @param enabled  consume the queue; off - only {@code POST /fetch} and gRPC
     * @param queue    durable queue of requests, declared on start if absent
     * @param ack      when a request is acknowledged
     * @param prefetch requests in work at once per instance, including those waiting for the crawl-delay of their
     *                 host
     */
    public record Rabbit(
        boolean enabled,
        String queue,
        Ack ack,
        int prefetch
    ) {
    }

    /**
     * When a request is acknowledged - which way of handling the queue gets.
     */
    public enum Ack {
        /**
         * After its reply is confirmed by the broker; the request waits for its page on the url-manager that took it.
         * For the in-memory queue of tasks: a request of a stopped url-manager stays in RabbitMQ; the requests of
         * a host must come to one url-manager (consistent-hash exchange by host). Works in the cluster too.
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
