package io.github.deripas.crawler.urlmanager.app;

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
import io.github.deripas.taskgrid.worker.remote.RemoteSettings;
import io.github.deripas.taskgrid.worker.remote.RemoteWorker;
import io.github.deripas.taskgrid.worker.remote.grpc.NodeService;
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

/**
 * Assembly: settings -> task-grid node (queue, results, remote worker, gRPC server) -> adapters -> fetch -> API.
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
        final RemoteWorker[] worker = new RemoteWorker[1];
        final MemoryTaskQueue queue = new MemoryTaskQueue(new QueueSettings(properties.maxAttempts()), results, callback -> {
            worker[0] = new RemoteWorker(RemoteSettings.DEFAULTS, callback, Clock.systemUTC());
            return worker[0];
        });
        return new Node(queue, worker[0]);
    }

    @Bean
    NodeServer nodeServer(UrlManagerProperties properties, Node node) {
        return new NodeServer(properties.grpcPort(), new NodeService(node.worker()), properties.shutdownTimeout());
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
}
