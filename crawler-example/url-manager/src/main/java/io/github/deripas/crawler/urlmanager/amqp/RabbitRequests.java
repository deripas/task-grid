package io.github.deripas.crawler.urlmanager.amqp;

import io.github.deripas.taskgrid.gateway.amqp.AmqpGateway;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.SmartLifecycle;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.concurrent.TimeoutException;

/**
 * The request queue of RabbitMQ in the lifecycle of the application: consumed once the node is up, closed first on
 * a stop - before the web server and the gRPC server of the sidecars - so that no new request comes in while the
 * node stops; requests not acknowledged go back to the queue.
 */
@Slf4j
@RequiredArgsConstructor
public final class RabbitRequests implements SmartLifecycle {
    private final AmqpGateway gateway;
    private volatile boolean running;

    @Override
    public void start() {
        try {
            gateway.start();
        } catch (IOException e) {
            throw new UncheckedIOException("RabbitMQ requests not consumed", e);
        } catch (TimeoutException e) {
            throw new IllegalStateException("RabbitMQ not reached", e);
        }
        running = true;
    }

    @Override
    public void stop() {
        gateway.close();
        running = false;
    }

    @Override
    public boolean isRunning() {
        return running;
    }
}
