package io.github.deripas.crawler.fetcher.app;

import io.github.deripas.crawler.fetcher.config.FetcherSettings;
import io.github.deripas.crawler.fetcher.grpc.FetcherService;
import io.github.deripas.crawler.fetcher.http.ApacheFetcher;
import io.grpc.Grpc;
import io.grpc.InsecureServerCredentials;
import io.grpc.Server;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * Assembly of the fetcher: settings from the environment, HTTP client, gRPC server on virtual threads.
 * On SIGTERM the server takes no new calls and finishes those in progress within SHUTDOWN_TIMEOUT
 * (the sidecar drains in front of it).
 */
@Slf4j
public final class Main {
    private Main() {
    }

    public static void main(String[] args) throws IOException, InterruptedException {
        final FetcherSettings settings = FetcherSettings.from(System.getenv());
        final ApacheFetcher fetcher = new ApacheFetcher(settings.connectTimeout(), settings.responseTimeout(), settings.maxBody());
        final ExecutorService calls = Executors.newVirtualThreadPerTaskExecutor();
        final Server server = Grpc.newServerBuilderForPort(settings.port(), InsecureServerCredentials.create())
            .executor(calls)
            .addService(new FetcherService(fetcher))
            .build()
            .start();
        log.info("Fetcher started: port={} settings={}", server.getPort(), settings);
        Runtime.getRuntime().addShutdownHook(new Thread(() -> stop(server, calls, fetcher, settings), "shutdown"));
        server.awaitTermination();
    }

    private static void stop(
        Server server,
        ExecutorService calls,
        ApacheFetcher fetcher,
        FetcherSettings settings
    ) {
        log.info("Fetcher stopping");
        server.shutdown();
        try {
            if (!server.awaitTermination(settings.shutdownTimeout().toMillis(), TimeUnit.MILLISECONDS)) {
                log.warn("Calls in progress abandoned after {}", settings.shutdownTimeout());
                server.shutdownNow();
            }
            calls.shutdown();
            fetcher.close();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (IOException e) {
            log.warn("HTTP client did not close", e);
        }
        log.info("Fetcher stopped");
    }
}
