package io.github.deripas.crawler.urlmanager.app;

import io.github.deripas.crawler.urlmanager.amqp.RabbitRequests;
import io.github.deripas.crawler.urlmanager.api.FetchController;
import io.github.deripas.crawler.urlmanager.blob.S3BlobReader;
import io.github.deripas.crawler.urlmanager.config.UrlManagerProperties;
import io.github.deripas.crawler.urlmanager.fetch.BlobReader;
import io.github.deripas.crawler.urlmanager.fetch.FetchService;
import io.github.deripas.crawler.urlmanager.fetcher.ProtoPageCodec;
import io.github.deripas.crawler.urlmanager.grpc.NodeServer;
import io.github.deripas.crawler.urlmanager.metrics.PoolMetrics;
import io.github.deripas.crawler.urlmanager.protocol.FetchTaskCodec;
import io.github.deripas.crawler.urlmanager.protocol.Replies;
import io.github.deripas.taskgrid.dispatch.Dispatcher;
import io.github.deripas.taskgrid.gateway.Gateway;
import io.github.deripas.taskgrid.gateway.TaskCodec;
import io.github.deripas.taskgrid.gateway.amqp.Ack;
import io.github.deripas.taskgrid.gateway.amqp.AmqpGateway;
import io.github.deripas.taskgrid.gateway.amqp.AmqpSettings;
import io.github.deripas.taskgrid.gateway.grpc.GatewayService;
import io.github.deripas.taskgrid.mailbox.Mailbox;
import io.github.deripas.taskgrid.mailbox.MailboxSettings;
import io.github.deripas.taskgrid.mailbox.MailboxSink;
import io.github.deripas.taskgrid.mailbox.memory.MemoryMailbox;
import io.github.deripas.taskgrid.mailbox.pekko.PekkoMailbox;
import io.github.deripas.taskgrid.queue.DelayPolicy;
import io.github.deripas.taskgrid.queue.QueueSettings;
import io.github.deripas.taskgrid.queue.TaskQueue;
import io.github.deripas.taskgrid.queue.memory.MemoryTaskQueue;
import io.github.deripas.taskgrid.queue.pekko.PekkoTaskQueue;
import io.github.deripas.taskgrid.worker.remote.RemoteSettings;
import io.github.deripas.taskgrid.worker.remote.RemoteWorker;
import io.github.deripas.taskgrid.worker.remote.grpc.NodeService;
import io.grpc.BindableService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBooleanProperty;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.EnableScheduling;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.http.apache5.Apache5HttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;

import java.io.IOException;
import java.net.URI;
import java.time.Clock;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Assembly: settings -> task-grid node (queue with crawl-delay, dispatcher, remote worker, gRPC server, pool metrics)
 * and the mailbox -> adapters -> fetch -> API (HTTP; RabbitMQ when {@code url-manager.rabbit.enabled}).
 */
@Configuration(proxyBeanMethods = false)
@EnableConfigurationProperties(UrlManagerProperties.class)
@EnableScheduling
public class UrlManagerConfiguration {
    @Bean
    RemoteWorker remoteWorker() {
        return new RemoteWorker(RemoteSettings.DEFAULTS, Clock.systemUTC());
    }

    /**
     * The queue of {@code url-manager.queue} connected to the worker and the mailbox; before the node joins the
     * cluster, so that the partitions recovering here find them.
     */
    @Bean
    Node node(
        TaskQueue queue,
        RemoteWorker remote,
        Mailbox mailbox
    ) {
        queue.onResult(new MailboxSink(mailbox));
        Dispatcher.connect(queue, remote);
        return new Node(queue, remote);
    }

    @Bean
    PoolMetrics poolMetrics(Node node) {
        return new PoolMetrics(node.remote());
    }

    @Bean
    NodeService nodeService(Node node) {
        return new NodeService(node.remote());
    }

    /**
     * Hosts the gRPC services of the node: sidecars, and clients when the gateway is on.
     */
    @Bean
    NodeServer nodeServer(UrlManagerProperties properties, List<BindableService> services) {
        final UrlManagerProperties.GrpcKeepalive keepalive = properties.grpcKeepalive();
        return new NodeServer(properties.grpcPort(), services,
            new NodeServer.Keepalive(keepalive.time(), keepalive.timeout(), keepalive.permitTime()), properties.shutdownTimeout());
    }

    @Bean
    BlobReader blobReader(UrlManagerProperties properties) {
        final UrlManagerProperties.Blob blob = properties.blob();
        if (!blob.configured()) {
            return ref -> {
                throw new IOException("no blob storage configured, cannot read " + ref.location());
            };
        }
        return new S3BlobReader(s3(blob));
    }

    /**
     * A virtual thread per reply of the gateway being made (a page may be read from blob storage).
     */
    @Bean(destroyMethod = "shutdownNow")
    ExecutorService gatewayThreads() {
        return Executors.newVirtualThreadPerTaskExecutor();
    }

    /**
     * Clients over gRPC ({@code taskgrid.gateway.v1.TaskGateway}, payloads of {@code crawler.urlmanager.v1}) on the
     * port of the node; their results wait in the mailbox.
     */
    @Bean
    GatewayService gatewayService(
        Gateway gateway,
        TaskCodec codec,
        @Qualifier("gatewayThreads") ExecutorService gatewayThreads
    ) {
        return new GatewayService(gateway, codec, gatewayThreads);
    }

    /**
     * The entry of the clients: requests reserve their places in the boxes and go to the queue; a client waiting here
     * has its box of this node ({@code url-manager.node-id}).
     */
    @Bean
    Gateway gateway(
        Node node,
        Mailbox mailbox,
        UrlManagerProperties properties
    ) {
        return new Gateway(node.queue(), mailbox, properties.nodeId());
    }

    /**
     * {@code crawler.urlmanager.v1} over every transport of the clients.
     */
    @Bean
    FetchTaskCodec fetchTaskCodec(FetchService service) {
        return new FetchTaskCodec(service);
    }

    /**
     * {@code POST /fetch}: waits for its page through the box of this node.
     */
    @Bean
    FetchController fetchController(FetchService service, UrlManagerProperties properties) {
        return new FetchController(service, properties.defaultTimeout(), properties.maxTimeout());
    }

    @Bean
    FetchService fetchService(Gateway gateway, BlobReader blobs) {
        return new FetchService(gateway, new ProtoPageCodec(), blobs);
    }

    private static QueueSettings settings(UrlManagerProperties properties) {
        return new QueueSettings(properties.maxAttempts(), properties.attemptTimeout());
    }

    private static MailboxSettings boxes(UrlManagerProperties properties) {
        return new MailboxSettings(properties.boxLimit(), properties.resultTtl());
    }

    /**
     * Path-style addressing: SeaweedFS and MinIO serve buckets as paths.
     */
    private static S3Client s3(UrlManagerProperties.Blob blob) {
        final AwsCredentialsProvider credentials = blob.accessKey() == null || blob.accessKey().isBlank()
            ? DefaultCredentialsProvider.builder().build()
            : StaticCredentialsProvider.create(AwsBasicCredentials.create(blob.accessKey(), blob.secretKey()));
        return S3Client.builder()
            .endpointOverride(URI.create(blob.endpoint()))
            .region(Region.of(blob.region()))
            .forcePathStyle(true)
            .credentialsProvider(credentials)
            .httpClientBuilder(Apache5HttpClient.builder())
            .build();
    }

    /**
     * Tasks and boxes in this process: a client gets its results only from the url-manager it submitted to.
     */
    @Configuration(proxyBeanMethods = false)
    @ConditionalOnProperty(name = "url-manager.queue", havingValue = "memory", matchIfMissing = true)
    static class Memory {
        @Bean
        MemoryTaskQueue taskQueue(UrlManagerProperties properties) {
            return new MemoryTaskQueue(settings(properties), DelayPolicy.fixed(properties.crawlDelay()));
        }

        @Bean
        MemoryMailbox mailbox(UrlManagerProperties properties) {
            return new MemoryMailbox(boxes(properties), Clock.systemUTC());
        }
    }

    /**
     * Tasks and boxes in the Pekko cluster of url-managers, journal in ScyllaDB: a result finishes on the node of its
     * partition and waits in its box, which any node reaches.
     */
    @Slf4j
    @Configuration(proxyBeanMethods = false)
    @ConditionalOnProperty(name = "url-manager.queue", havingValue = "cluster")
    static class Cluster {
        @Bean
        PekkoCluster pekkoCluster(UrlManagerProperties properties) {
            log.info("Cluster mode: tasks and boxes in the Pekko cluster");
            return new PekkoCluster(properties.cluster());
        }

        @Bean
        PekkoTaskQueue taskQueue(PekkoCluster cluster, UrlManagerProperties properties) {
            return PekkoTaskQueue.start(cluster.system(), settings(properties), DelayPolicy.fixed(properties.crawlDelay()));
        }

        @Bean
        PekkoMailbox mailbox(PekkoCluster cluster, UrlManagerProperties properties) {
            return PekkoMailbox.start(cluster.system(), boxes(properties));
        }
    }

    /**
     * Requests over RabbitMQ ({@code crawler.urlmanager.v1}): the gateway of task-grid on its own connection, made by
     * the connection factory of Spring Boot ({@code spring.rabbitmq.*}); the way of handling is
     * {@code url-manager.rabbit.ack}.
     */
    @Configuration(proxyBeanMethods = false)
    @ConditionalOnBooleanProperty("url-manager.rabbit.enabled")
    static class Rabbit {
        /**
         * A virtual thread per request in work and per reply.
         */
        @Bean(destroyMethod = "shutdownNow")
        ExecutorService fetchRequestThreads() {
            return Executors.newVirtualThreadPerTaskExecutor();
        }

        /**
         * Created with the node: after-submit registers its result callback before the node takes tasks.
         */
        @Bean
        AmqpGateway amqpGateway(
            CachingConnectionFactory connections,
            Node node,
            Gateway gateway,
            TaskCodec codec,
            @Qualifier("fetchRequestThreads") ExecutorService fetchRequestThreads,
            UrlManagerProperties properties
        ) {
            final UrlManagerProperties.Rabbit rabbit = properties.rabbit();
            final Ack ack = switch (rabbit.ack()) {
                case AFTER_REPLY -> Ack.AFTER_REPLY;
                case AFTER_SUBMIT -> Ack.AFTER_SUBMIT;
            };
            final AmqpSettings settings = new AmqpSettings(rabbit.queue(), rabbit.prefetch(), ack, properties.defaultTimeout(),
                Replies.CONTENT_TYPE);
            return new AmqpGateway(connections.getRabbitConnectionFactory().clone(), settings, node.queue(), gateway, codec,
                fetchRequestThreads);
        }

        @Bean
        RabbitRequests rabbitRequests(AmqpGateway amqpGateway) {
            return new RabbitRequests(amqpGateway);
        }
    }
}
