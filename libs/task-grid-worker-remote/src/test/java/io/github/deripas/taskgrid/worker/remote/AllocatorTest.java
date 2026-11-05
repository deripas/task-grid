package io.github.deripas.taskgrid.worker.remote;

import io.github.deripas.taskgrid.task.Attempt;
import io.github.deripas.taskgrid.task.AttemptResult;
import io.github.deripas.taskgrid.task.AttemptStatus;
import io.github.deripas.taskgrid.task.Blob;
import io.github.deripas.taskgrid.task.Task;
import io.github.deripas.taskgrid.worker.Assignment;
import io.github.deripas.taskgrid.worker.Worker;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Distribution of attempts of one kind: demand, credit, return of surplus credit, HRW affinity and its widening,
 * attempts of lost, superseded and restarted sidecars, failed writes, node stop. The state machine without an engine:
 * time is a parameter.
 */
class AllocatorTest {
    private static final long T0 = 1_000_000;
    private static final RemoteSettings SETTINGS = new RemoteSettings(
        Duration.ofMillis(10), Duration.ofMillis(150), Duration.ofMillis(20), Duration.ofSeconds(2), Duration.ofSeconds(1));
    private static final long REJOIN = SETTINGS.rejoinTimeout().toMillis();

    private final List<AttemptResult> retried = new ArrayList<>();
    private final Allocator allocator = new Allocator("k", SETTINGS, result -> {
        retried.add(result);
        return CompletableFuture.completedFuture(null);
    });
    private int sessions;
    private int tasks;

    @Test
    void attemptWaitsForCreditAndDemandAboveCapacityIsStepped() {
        final Sidecar s = join("w1", 2);
        assign(null);
        assign(null);
        assign(null); // 3 waiting: step 2 x 1, demand does not change
        assign(null); // 4: step 2 x 2
        assertEquals(List.of(0L, 1L, 2L, 4L), s.targets());
        assertTrue(s.channel.attempts.isEmpty());

        s.credit(1, 1);
        s.credit(2, 2);
        assertEquals(List.of("t1", "t2"), s.taskIds());
    }

    @Test
    void surplusCreditIsReturnedAndNotUsedUntilNewVersion() {
        final Sidecar s = join("w1", 4);

        s.credit(3, 1); // credit without attempts
        assertEquals(new Demand(0, 1), s.channel.demands.getLast());

        assign(null); // credit of version 1 was returned: the attempt waits for a new version
        assertEquals(1, s.target());
        assertTrue(s.channel.attempts.isEmpty());
        s.credit(1, 2);
        assertEquals(List.of("t1"), s.taskIds());
    }

    @Test
    void affinityKeyAsksOnlyPreferredSidecar() {
        final List<Sidecar> sidecars = List.of(join("w1", 10), join("w2", 10), join("w3", 10));
        final String preferred = best("same-key", "w1", "w2", "w3");
        final Sidecar top = sidecars.stream().filter(s -> s.workerId.equals(preferred)).findFirst().orElseThrow();

        for (int i = 0; i < 3; i++) {
            assign("same-key");
        }
        top.credit(3, 1);
        assertEquals(3, top.channel.attempts.size());
        // sent before affinity-wait: no demand for the other sidecars
        for (Sidecar s : sidecars) {
            if (s != top) {
                assertEquals(List.of(0L), s.targets());
            }
        }
    }

    @Test
    void affinityKeyWidensToOtherSidecarsAfterWait() {
        final Sidecar w1 = join("w1", 1);
        final Sidecar w2 = join("w2", 1);
        assign(keyPreferring("w1", "w1", "w2"));
        assertEquals(1, w1.target());
        assertEquals(0, w2.target());

        final long wait = SETTINGS.affinityWait().toMillis();
        allocator.tick(T0 + wait - 1);
        assertEquals(0, w2.target());
        allocator.tick(T0 + wait); // no credit from w1: demand for w2 too
        assertEquals(1, w2.target());

        w2.credit(1, 1);
        assertEquals(List.of("t1"), w2.taskIds());
        assertEquals(0, w1.target()); // the attempt is gone: demand for w1 is withdrawn
    }

    @Test
    void withoutAffinityKeyCreditOfAnySidecarIsTaken() {
        final List<Sidecar> sidecars = List.of(join("w1", 1), join("w2", 1), join("w3", 1));
        assign(null);
        assertEquals(2, sidecars.stream().filter(s -> s.target() == 1).count()); // two random
        allocator.tick(T0 + SETTINGS.spreadWait().toMillis());
        sidecars.forEach(s -> assertEquals(1, s.target())); // after spread-wait - all

        sidecars.getLast().credit(1, 1);
        assertEquals(List.of("t1"), sidecars.getLast().taskIds());
    }

    @Test
    void sentAttemptIsAcceptedWithWholeMeta() {
        final Sidecar s = join("w1", 1);
        final CompletableFuture<Assignment> reply = assign("x");
        s.credit(1, 1);
        assertEquals(new Assignment.Accepted(), reply.join());
        assertEquals(Map.of("kind", "k", Worker.AFFINITY_KEY, "x"), s.channel.attempts.getFirst().task().meta());
    }

    @Test
    void failedSendClosesSessionAndGoesToAnotherSidecar() {
        final Sidecar broken = join("w1", 1);
        final Sidecar ok = join("w2", 1);
        broken.channel.refuseAttempts = true;
        final CompletableFuture<Assignment> reply = assign(null);

        broken.credit(1, 1);
        assertTrue(broken.channel.closed);
        assertEquals(1, allocator.sessions());
        assertFalse(reply.isDone());

        ok.credit(1, 1);
        assertEquals(List.of("t1"), ok.taskIds());
        assertEquals(new Assignment.Accepted(), reply.join());
    }

    @Test
    void failedDemandClosesSession() {
        final Sidecar s = join("w1", 1);
        s.channel.refuseAll = true;
        assign(null);
        assertTrue(s.channel.closed);
        assertEquals(0, allocator.sessions());
    }

    /**
     * The session broke and the sidecar did not come back: its attempt goes to another sidecar with the same number,
     * the queue does not see it.
     */
    @Test
    void attemptsOfALostSidecarGoToAnotherAfterRejoinTimeout() {
        final Sidecar s = join("w1", 2);
        assign(null);
        assign(null);
        s.credit(2, 1);
        allocator.completed("w1", "t1", 1);

        allocator.leave(s.channel.sessionId, "broken", T0);
        allocator.tick(T0 + REJOIN);
        assertEquals(new Load(0, 1, 0), allocator.load(), "waits for the sidecar to reconnect");

        allocator.tick(T0 + REJOIN + 1);
        assertEquals(new Load(1, 0, 0), allocator.load());
        final Sidecar other = join("w2", 1);
        other.credit(1, 1);
        assertEquals(List.of("t2"), other.taskIds());
        assertEquals(1, other.channel.attempts.getFirst().number());
        assertTrue(retried.isEmpty(), "nothing goes to the queue: " + retried);
    }

    @Test
    void sidecarThatReconnectsKeepsItsAttempts() {
        final Sidecar s = join("w1", 1);
        assign(null);
        s.credit(1, 1);
        allocator.leave(s.channel.sessionId, "broken", T0);

        join("w1", 1);
        allocator.tick(T0 + REJOIN + 1);
        assertEquals(new Load(0, 1, 1), allocator.load());
        allocator.completed("w1", "t1", 1);
        assertEquals(new Load(0, 0, 1), allocator.load());
    }

    /**
     * The same worker id with another instance id: the process was restarted and lost its attempts.
     */
    @Test
    void restartedSidecarLosesItsAttemptsAtOnce() {
        final Sidecar old = join("w1", "i1", 1);
        assign(null);
        old.credit(1, 1);

        final Sidecar fresh = join("w1", "i2", 1);
        assertTrue(old.channel.closed);
        fresh.credit(1, 1);
        assertEquals(List.of("t1"), fresh.taskIds());
        assertTrue(retried.isEmpty(), "nothing goes to the queue: " + retried);
    }

    /**
     * A draining sidecar that breaks the session has abandoned its attempts: they are not waited for.
     */
    @Test
    void sidecarLostWhileDrainingIsNotWaitedFor() {
        final Sidecar s = join("w1", 1);
        assign(null);
        s.credit(1, 1);
        allocator.drain(s.channel.sessionId, T0);

        allocator.leave(s.channel.sessionId, "drain timeout", T0);
        assertEquals(new Load(1, 0, 0), allocator.load());
    }

    @Test
    void drainingSessionGetsNoAttemptsAndEndsWithItsLastResult() {
        final Sidecar draining = join("w1", 2);
        assign(null);
        draining.credit(2, 1);
        assertEquals(List.of("t1"), draining.taskIds());

        allocator.drain(draining.channel.sessionId, T0);
        assign(null);
        assertEquals(List.of("t1"), draining.taskIds(), "credit of a draining session is not used");
        assertFalse(draining.channel.closed);
        assertEquals(1, allocator.sessions());

        allocator.completed("w1", "t1", 1);
        assertTrue(draining.channel.closed);
        assertEquals(0, allocator.sessions());
        assertTrue(retried.isEmpty(), "nothing to retry: " + retried);
    }

    @Test
    void idleSessionEndsAtOnceOnDrain() {
        final Sidecar s = join("w1", 1);
        allocator.drain(s.channel.sessionId, T0);
        assertTrue(s.channel.closed);
        assertEquals(0, allocator.sessions());
    }

    @Test
    void newSessionOfSameWorkerSupersedesPrevious() {
        final Sidecar old = join("w1", 1);
        assign(null);
        old.credit(1, 1);

        final Sidecar fresh = join("w1", 1);
        assertTrue(old.channel.closed, "superseded");
        assertFalse(fresh.channel.closed);
        assertEquals(1, allocator.sessions());
        assertTrue(retried.isEmpty(), "the attempt stays in progress: " + retried);

        allocator.completed("w1", "t1", 1); // received in the old session, completed in the new one
        assertEquals(new Load(0, 0, 1), allocator.load());
    }

    @Test
    void resultOfAnotherAttemptDoesNotClearAttemptInProgress() {
        final Sidecar s = join("w1", 1);
        assign(null);
        s.credit(1, 1);
        allocator.completed("w1", "t1", 2);

        assertEquals(new Load(0, 1, 1), allocator.load());
    }

    /**
     * Attempts in progress are left to their sidecars (the results go to another node); waiting ones are retried.
     */
    @Test
    void stopClosesSessionsAndRetriesWaiting() {
        final Sidecar s = join("w1", 1);
        final CompletableFuture<Assignment> reply = assign(null);
        allocator.stop();
        assertEquals(new Assignment.Accepted(), reply.join());
        assertTrue(s.channel.closed);
        assertEquals(List.of("node stopping"), retried.stream().map(AttemptResult::error).toList());

        assertInstanceOf(Assignment.Accepted.class, assign(null).join());
        assertEquals(2, retried.size());
    }

    /**
     * No sidecar of the kind for too long: the attempt counts as a technical failure, the queue retries it.
     */
    @Test
    void retriedAfterAssignTimeout() {
        final CompletableFuture<Assignment> reply = assign(null);
        allocator.tick(T0 + SETTINGS.assignTimeout().toMillis());
        assertFalse(reply.isDone());
        allocator.tick(T0 + SETTINGS.assignTimeout().toMillis() + 1);
        assertInstanceOf(Assignment.Accepted.class, reply.join());
        assertEquals(AttemptStatus.RETRY, retried.getFirst().status());
        assertEquals(new Load(0, 0, 0), allocator.load());
    }

    @Test
    void loadCountsWaitingBusyAndCapacity() {
        final Sidecar a = join("w1", 2);
        join("w2", 3);
        assign(null);
        assign(null);
        assign(null);
        assertEquals(new Load(3, 0, 5), allocator.load());

        a.credit(2, 1);
        assertEquals(new Load(1, 2, 5), allocator.load());

        allocator.completed("w1", a.taskIds().getFirst(), 1);
        assertEquals(new Load(1, 1, 5), allocator.load());
    }

    /**
     * A sidecar connected to several nodes tells each its share of the capacity.
     */
    @Test
    void capacityIsTheShareOfTheSidecar() {
        final Sidecar a = join("w1", 4);
        join("w2", 3);
        allocator.shared(a.channel.sessionId, 4 / 3.0);
        assertEquals(4 / 3.0 + 3, allocator.load().capacity(), 1e-9);
    }

    private Sidecar join(String workerId, int capacity) {
        return join(workerId, "i-" + workerId, capacity);
    }

    private Sidecar join(
        String workerId,
        String instanceId,
        int capacity
    ) {
        final Channel channel = new Channel("s" + (++sessions));
        allocator.join(channel.sessionId, new SidecarHello(workerId, instanceId, "k", capacity), channel, T0);
        return new Sidecar(workerId, channel);
    }

    /**
     * @param key affinity key, {@code null} - none
     */
    private CompletableFuture<Assignment> assign(String key) {
        final Map<String, String> meta = key == null
            ? Map.of("kind", "k")
            : Map.of("kind", "k", Worker.AFFINITY_KEY, key);
        final Task task = new Task("t" + (++tasks), Blob.of(new byte[0]), meta);
        final CompletableFuture<Assignment> reply = new CompletableFuture<>();
        allocator.assign(new Attempt(task, 1), reply, T0);
        return reply;
    }

    private static String best(String key, String... workers) {
        String best = null;
        for (String w : workers) {
            if (best == null || Long.compareUnsigned(Hrw.score(key, w), Hrw.score(key, best)) > 0) {
                best = w;
            }
        }
        return best;
    }

    private static String keyPreferring(String worker, String... workers) {
        for (int i = 0; ; i++) {
            if (best("key-" + i, workers).equals(worker)) {
                return "key-" + i;
            }
        }
    }

    private record Demand(long target, long version) {
    }

    /**
     * A sidecar on the test side: what the allocator wrote to its channel, and credit on its behalf.
     */
    private final class Sidecar {
        final String workerId;
        final Channel channel;

        Sidecar(String workerId, Channel channel) {
            this.workerId = workerId;
            this.channel = channel;
        }

        void credit(long limit, long version) {
            allocator.credited(channel.sessionId, limit, version, T0);
        }

        long target() {
            return channel.demands.getLast().target();
        }

        List<Long> targets() {
            return channel.demands.stream().map(Demand::target).toList();
        }

        List<String> taskIds() {
            return channel.attempts.stream().map(a -> a.task().id()).toList();
        }
    }

    /**
     * Probe channel: refuseAttempts - attempts are not sent, refuseAll - nothing is sent.
     */
    private static final class Channel implements SessionChannel {
        final String sessionId;
        final List<Attempt> attempts = new ArrayList<>();
        final List<Demand> demands = new ArrayList<>();
        boolean refuseAttempts;
        boolean refuseAll;
        boolean closed;

        Channel(String sessionId) {
            this.sessionId = sessionId;
        }

        @Override
        public boolean attempt(Attempt attempt) {
            if (refuseAttempts || refuseAll) {
                return false;
            }
            attempts.add(attempt);
            return true;
        }

        @Override
        public boolean demand(long target, long version) {
            if (refuseAll) {
                return false;
            }
            demands.add(new Demand(target, version));
            return true;
        }

        @Override
        public void close() {
            closed = true;
        }
    }
}
