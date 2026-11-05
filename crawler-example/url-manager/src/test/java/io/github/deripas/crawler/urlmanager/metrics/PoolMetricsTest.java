package io.github.deripas.crawler.urlmanager.metrics;

import io.github.deripas.taskgrid.task.Attempt;
import io.github.deripas.taskgrid.task.Blob;
import io.github.deripas.taskgrid.task.Task;
import io.github.deripas.taskgrid.worker.Worker;
import io.github.deripas.taskgrid.worker.remote.RemoteSettings;
import io.github.deripas.taskgrid.worker.remote.RemoteWorker;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.time.Clock;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Gauges of a kind appear once the remote worker has seen the kind; values are read from the worker at scrape time.
 */
class PoolMetricsTest {
    private final RemoteWorker worker = new RemoteWorker(RemoteSettings.DEFAULTS, Clock.systemUTC());
    private final MeterRegistry registry = new SimpleMeterRegistry();
    private final PoolMetrics metrics = new PoolMetrics(worker);

    @AfterEach
    void stop() {
        worker.close();
    }

    @Test
    void newKindGetsItsGauges() throws InterruptedException {
        metrics.bindTo(registry);
        worker.assign(new Attempt(new Task("t", Blob.of(new byte[0]), Map.of(Worker.KIND, "fetch")), 1));
        // load is refreshed on the tick of the worker
        for (int i = 0; i < 500 && worker.load().isEmpty(); i++) {
            Thread.sleep(10);
        }
        metrics.refresh();

        assertEquals(1.0, registry.get("task.grid.worker.waiting").tag("kind", "fetch").gauge().value());
        assertEquals(0.0, registry.get("task.grid.worker.busy").tag("kind", "fetch").gauge().value());
        assertEquals(0.0, registry.get("task.grid.worker.capacity").tag("kind", "fetch").gauge().value());
    }
}
