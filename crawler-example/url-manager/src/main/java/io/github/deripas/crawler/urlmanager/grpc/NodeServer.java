package io.github.deripas.crawler.urlmanager.grpc;

import io.github.deripas.taskgrid.worker.remote.grpc.NodeService;
import io.grpc.Grpc;
import io.grpc.InsecureServerCredentials;
import io.grpc.Server;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.SmartLifecycle;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.time.Duration;
import java.util.concurrent.TimeUnit;

/**
 * gRPC server of the task-grid node: sidecars connect to it ({@code taskgrid.worker.remote.v1.Node}).
 * <p>
 * Starts before the web server and stops after it: requests of clients still waiting for pages need the sidecars.
 * By then nobody waits for results, so the sessions are cancelled at once (the queue is in memory): sidecars
 * reconnect to the next node.
 */
@Slf4j
public final class NodeServer implements SmartLifecycle {
    /**
     * Below the phases of the web server (graceful shutdown, start / stop).
     */
    static final int PHASE = SmartLifecycle.DEFAULT_PHASE - 4096;

    private final int port;
    private final NodeService service;
    private final Duration shutdownTimeout;
    private volatile Server server;

    /**
     * @param port 0 - any free one, see {@link #port()}
     */
    public NodeServer(
        int port,
        NodeService service,
        Duration shutdownTimeout
    ) {
        this.port = port;
        this.service = service;
        this.shutdownTimeout = shutdownTimeout;
    }

    @Override
    public void start() {
        try {
            server = Grpc.newServerBuilderForPort(port, InsecureServerCredentials.create())
                .addService(service)
                .build()
                .start();
        } catch (IOException e) {
            throw new UncheckedIOException("gRPC server on port " + port, e);
        }
        log.info("Node gRPC server started: port={}", server.getPort());
    }

    @Override
    public void stop() {
        final Server running = server;
        if (running == null) {
            return;
        }
        running.shutdownNow();
        try {
            if (!running.awaitTermination(shutdownTimeout.toMillis(), TimeUnit.MILLISECONDS)) {
                log.warn("Node gRPC server did not terminate within {}", shutdownTimeout);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        server = null;
        log.info("Node gRPC server stopped");
    }

    @Override
    public boolean isRunning() {
        return server != null;
    }

    @Override
    public int getPhase() {
        return PHASE;
    }

    /**
     * The port it listens on, once started.
     */
    public int port() {
        return server.getPort();
    }
}
