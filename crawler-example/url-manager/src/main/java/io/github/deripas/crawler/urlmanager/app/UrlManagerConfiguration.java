package io.github.deripas.crawler.urlmanager.app;

import io.github.deripas.crawler.urlmanager.amqp.AckAfterReplyRequests;
import io.github.deripas.crawler.urlmanager.amqp.AckAfterSubmitRequests;
import io.github.deripas.crawler.urlmanager.amqp.FetchRequests;
import io.github.deripas.crawler.urlmanager.amqp.RabbitFetchListener;
import io.github.deripas.crawler.urlmanager.amqp.RabbitReplyPublisher;
import io.github.deripas.crawler.urlmanager.api.FetchController;
import io.github.deripas.crawler.urlmanager.blob.S3BlobReader;
import io.github.deripas.crawler.urlmanager.config.UrlManagerProperties;
import io.github.deripas.crawler.urlmanager.fetch.BlobReader;
import io.github.deripas.crawler.urlmanager.fetch.FetchService;
import io.github.deripas.crawler.urlmanager.fetcher.ProtoPageCodec;
import io.github.deripas.crawler.urlmanager.grpc.NodeServer;
import io.github.deripas.taskgrid.queue.QueueSettings;
import io.github.deripas.taskgrid.queue.memory.MemoryTaskQueue;
import io.github.deripas.taskgrid.result.ResultStore;
import io.github.deripas.taskgrid.result.memory.MemoryResultStore;
import io.github.deripas.taskgrid.worker.delayed.DelayedWorker;
import io.github.deripas.taskgrid.worker.delayed.FixedDelay;
import io.github.deripas.taskgrid.worker.remote.RemoteSettings;
import io.github.deripas.taskgrid.worker.remote.RemoteWorker;
import io.github.deripas.taskgrid.worker.remote.grpc.NodeService;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.core.QueueBuilder;
import org.springframework.amqp.rabbit.config.SimpleRabbitListenerContainerFactory;
import org.springframework.amqp.rabbit.config.SimpleRabbitListenerEndpoint;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.rabbit.listener.SimpleMessageListenerContainer;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBooleanProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Assembly: settings -> task-grid node (queue, results, crawl-delay, remote worker, gRPC server) -> adapters -> fetch
 * -> API (HTTP; RabbitMQ when {@code url-manager.rabbit.enabled}).
 */
@Configuration(proxyBeanMethods = false)
@EnableConfigurationProperties(UrlManagerProperties.class)
public class UrlManagerConfiguration {
    @Bean
    ResultStore results(UrlManagerProperties properties) {
        return new MemoryResultStore(properties.resultTtl(), Clock.systemUTC());
    }

    @Bean
    Node node(UrlManagerProperties properties, ResultStore results) {
        final DelayedWorker[] delayed = new DelayedWorker[1];
        final RemoteWorker[] remote = new RemoteWorker[1];
        final MemoryTaskQueue queue = new MemoryTaskQueue(new QueueSettings(properties.maxAttempts()), results, callback -> {
            delayed[0] = new DelayedWorker(new FixedDelay(properties.crawlDelay()), callback, inner -> {
                remote[0] = new RemoteWorker(RemoteSettings.DEFAULTS, inner, Clock.systemUTC());
                return remote[0];
            });
            return delayed[0];
        });
        return new Node(queue, delayed[0], remote[0]);
    }

    @Bean
    NodeServer nodeServer(UrlManagerProperties properties, Node node) {
        return new NodeServer(properties.grpcPort(), new NodeService(node.remote()), properties.shutdownTimeout());
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

    @Bean
    FetchService fetchService(Node node, ResultStore results, BlobReader blobs) {
        return new FetchService(node.queue(), results, new ProtoPageCodec(), blobs);
    }

    @Bean
    FetchController fetchController(FetchService service, UrlManagerProperties properties) {
        return new FetchController(service, properties.defaultTimeout(), properties.maxTimeout());
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
     * Requests over RabbitMQ: the queue, a consumer with manual acknowledgement, replies with publisher confirms.
     * The connection and the template are those of Spring Boot ({@code spring.rabbitmq.*}); the way of handling is
     * {@code url-manager.rabbit.ack}.
     */
    @Configuration(proxyBeanMethods = false)
    @ConditionalOnBooleanProperty("url-manager.rabbit.enabled")
    static class Rabbit {
        @Bean
        Queue fetchQueue(UrlManagerProperties properties) {
            return QueueBuilder.durable(properties.rabbit().queue()).build();
        }

        /**
         * A virtual thread per request in work and per reply.
         */
        @Bean(destroyMethod = "shutdownNow")
        ExecutorService fetchRequestThreads() {
            return Executors.newVirtualThreadPerTaskExecutor();
        }

        @Bean
        FetchRequests fetchRequests(
            FetchService service,
            RabbitTemplate template,
            ExecutorService fetchRequestThreads,
            UrlManagerProperties properties
        ) {
            final RabbitReplyPublisher replies = new RabbitReplyPublisher(template);
            return switch (properties.rabbit().ack()) {
                case AFTER_REPLY -> new AckAfterReplyRequests(service, replies, fetchRequestThreads,
                    properties.defaultTimeout(), properties.maxTimeout());
                case AFTER_SUBMIT -> {
                    final AckAfterSubmitRequests requests = new AckAfterSubmitRequests(service, replies, fetchRequestThreads);
                    service.onFinished(requests);
                    yield requests;
                }
            };
        }

        @Bean
        SimpleMessageListenerContainer fetchListener(
            SimpleRabbitListenerContainerFactory factory,
            FetchRequests requests,
            UrlManagerProperties properties
        ) {
            final SimpleRabbitListenerEndpoint endpoint = new SimpleRabbitListenerEndpoint();
            endpoint.setId("fetch-requests");
            endpoint.setQueueNames(properties.rabbit().queue());
            endpoint.setMessageListener(new RabbitFetchListener(requests));
            return factory.createListenerContainer(endpoint);
        }
    }
}
