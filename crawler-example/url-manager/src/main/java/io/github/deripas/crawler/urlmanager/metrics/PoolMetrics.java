package io.github.deripas.crawler.urlmanager.metrics;

import io.github.deripas.taskgrid.worker.remote.Load;
import io.github.deripas.taskgrid.worker.remote.RemoteWorker;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.MultiGauge;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.binder.MeterBinder;
import lombok.RequiredArgsConstructor;
import org.springframework.scheduling.annotation.Scheduled;

import java.util.List;
import java.util.Set;
import java.util.function.ToIntFunction;

/**
 * Load of the worker pools by kind, the signals for autoscaling the pools (KEDA):
 * <ul>
 *   <li>{@code task_grid_worker_waiting} - attempts waiting for a free slot: tasks stand idle because the pool is too
 *   small; for latency-bound pools any value above zero means scale up;</li>
 *   <li>{@code task_grid_worker_busy} / {@code task_grid_worker_capacity} - utilization: throughput-bound pools keep
 *   it at a target (e.g. 80%).</li>
 * </ul>
 * Values are read at scrape time; the set of kinds is refreshed periodically, a new kind appears within a second.
 */
@RequiredArgsConstructor
public final class PoolMetrics implements MeterBinder {
    private static final String KIND = "kind";

    private final RemoteWorker worker;
    private MultiGauge waiting;
    private MultiGauge busy;
    private MultiGauge capacity;
    private Set<String> kinds = Set.of();

    @Override
    public synchronized void bindTo(MeterRegistry registry) {
        waiting = MultiGauge.builder("task.grid.worker.waiting")
            .description("Attempts waiting for a free slot of a handler")
            .register(registry);
        busy = MultiGauge.builder("task.grid.worker.busy")
            .description("Attempts in progress in the handlers")
            .register(registry);
        capacity = MultiGauge.builder("task.grid.worker.capacity")
            .description("Slots of the connected handlers")
            .register(registry);
        kinds = Set.of();
        refresh();
    }

    /**
     * Registers the kinds that appeared since the last call.
     */
    @Scheduled(fixedDelay = 1000)
    public synchronized void refresh() {
        final Set<String> current = worker.load().keySet();
        if (waiting == null || current.equals(kinds)) {
            return;
        }
        kinds = Set.copyOf(current);
        register(waiting, Load::waiting);
        register(busy, Load::busy);
        register(capacity, Load::capacity);
    }

    private void register(MultiGauge gauge, ToIntFunction<Load> value) {
        final List<MultiGauge.Row<?>> rows = kinds.stream()
            .<MultiGauge.Row<?>>map(kind -> MultiGauge.Row.of(Tags.of(KIND, kind), worker,
                w -> value.applyAsInt(w.load().getOrDefault(kind, Load.EMPTY))))
            .toList();
        gauge.register(rows, true);
    }
}
