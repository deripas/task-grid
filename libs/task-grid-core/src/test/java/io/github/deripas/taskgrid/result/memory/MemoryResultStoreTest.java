package io.github.deripas.taskgrid.result.memory;

import io.github.deripas.taskgrid.task.TaskResult;
import io.github.deripas.taskgrid.task.TaskStatus;
import org.junit.jupiter.api.Test;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class MemoryResultStoreTest {
    private final MutableClock clock = new MutableClock();
    private final MemoryResultStore store = new MemoryResultStore(Duration.ofMinutes(10), clock);

    @Test
    void awaitCompletesWhenTheResultIsPut() {
        final CompletableFuture<Optional<TaskResult>> waiter = store.await("a", Duration.ofSeconds(10)).toCompletableFuture();
        assertFalse(waiter.isDone());

        store.put(result("a"));
        assertEquals(Optional.of(result("a")), waiter.join());
    }

    @Test
    void awaitReturnsAResultPutEarlier() {
        store.put(result("a"));
        assertEquals(Optional.of(result("a")), store.await("a", Duration.ZERO).toCompletableFuture().join());
    }

    @Test
    void awaitIsEmptyAfterTheTimeout() {
        assertEquals(Optional.empty(), store.await("a", Duration.ofMillis(20)).toCompletableFuture().join());
        assertEquals(Optional.empty(), store.await("a", Duration.ZERO).toCompletableFuture().join());
    }

    @Test
    void resultExpiresAfterTtl() {
        store.put(result("a"));
        clock.advance(Duration.ofMinutes(5));
        store.put(result("b"));
        clock.advance(Duration.ofMinutes(6));

        assertTrue(store.await("a", Duration.ZERO).toCompletableFuture().join().isEmpty());
        assertTrue(store.await("b", Duration.ZERO).toCompletableFuture().join().isPresent());
    }

    private static TaskResult result(String taskId) {
        return new TaskResult(taskId, TaskStatus.DONE, null, null, Map.of());
    }

    private static final class MutableClock extends Clock {
        private Instant now = Instant.parse("2026-01-01T00:00:00Z");

        @Override
        public ZoneOffset getZone() {
            return ZoneOffset.UTC;
        }

        @Override
        public Clock withZone(ZoneId zone) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Instant instant() {
            return now;
        }

        private void advance(Duration duration) {
            now = now.plus(duration);
        }
    }
}
