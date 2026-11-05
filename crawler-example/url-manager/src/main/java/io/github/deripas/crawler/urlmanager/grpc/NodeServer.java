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
 * <p>
 * Keepalive: sessions live long and may be quiet. The server pings each connection and closes one that does not
 * answer, so a hung sidecar (the connection is there, nothing passes) loses its session and its attempts go to other
 * sidecars after the rejoin timeout;
 * it accepts the pings of sidecars, which detect a hung node the same way.
 */
@Slf4j
public final class NodeServer implements SmartLifecycle {
    /**
     * HTTP/2 pings of the connections.
     *
     * @param time       ping a connection quiet this long; gRPC raises it to 10s at least
     * @param timeout    no answer to a ping this long - the connection is closed
     * @param permitTime pings of a sidecar more often than this close its connection ({@code too_many_pings}): at
     *                   most the keepalive time of the sidecars ({@code NODE_KEEPALIVE_TIME}, 10s)
     */
    public record Keepalive(
        Duration time,
        Duration timeout,
        Duration permitTime
    ) {
    }

    /**
     * Below the phases of the web server (graceful shutdown, start / stop).
     */
    public static final int PHASE = SmartLifecycle.DEFAULT_PHASE - 4096;

    private final int port;
    private final NodeService service;
    private final Keepalive keepalive;
    private final Duration shutdownTimeout;
    private volatile Server server;

    /**
     * @param port 0 - any free one, see {@link #port()}
     */
    public NodeServer(
        int port,
        NodeService service,
        Keepalive keepalive,
        Duration shutdownTimeout
    ) {
        this.port = port;
        this.service = service;
        this.keepalive = keepalive;
        this.shutdownTimeout = shutdownTimeout;
    }

    @Override
    public void start() {
        try {
            server = Grpc.newServerBuilderForPort(port, InsecureServerCredentials.create())
                .addService(service)
                .keepAliveTime(keepalive.time().toNanos(), TimeUnit.NANOSECONDS)
                .keepAliveTimeout(keepalive.timeout().toNanos(), TimeUnit.NANOSECONDS)
                .permitKeepAliveTime(keepalive.permitTime().toNanos(), TimeUnit.NANOSECONDS)
                .permitKeepAliveWithoutCalls(true)
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
