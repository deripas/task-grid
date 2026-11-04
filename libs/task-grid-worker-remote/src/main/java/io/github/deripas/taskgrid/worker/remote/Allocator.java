package io.github.deripas.taskgrid.worker.remote;

import io.github.deripas.taskgrid.task.Attempt;
import io.github.deripas.taskgrid.task.AttemptResult;
import io.github.deripas.taskgrid.task.Task;
import io.github.deripas.taskgrid.worker.Assignment;
import io.github.deripas.taskgrid.worker.AttemptCallback;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Distribution of the attempts of one kind among the sidecars connected to this node: an attempt is sent to
 * a sidecar only when that sidecar has granted credit.
 * <ul>
 *   <li>An attempt waits for credit of its candidates. Demand for a sidecar - how many waiting attempts name it
 *   (above capacity - in steps); the sidecar grants free slots as credit.</li>
 *   <li>With an affinity key ({@link Task#AFFINITY_KEY}): the candidate is the best sidecar by HRW(key, worker id);
 *   no credit within affinity-wait - the three best, within twice as long - all.</li>
 *   <li>Without one: two random sidecars, after spread-wait - all; credit of any sidecar takes such an attempt.</li>
 *   <li>Credit without a matching attempt is returned (Demand with target below limit).</li>
 *   <li>A session remembers its attempts in progress until their results: when it ends, they are retried.</li>
 *   <li>A draining session gets no more attempts and ends when its attempts in progress are completed.</li>
 *   <li>A failed write to a session channel closes the session; the attempt is distributed again.</li>
 * </ul>
 * Pure state machine: no threads, time is a parameter. Methods are called sequentially, one thread at a time;
 * events and the periodic {@link #tick} are delivered by the engine ({@link RemoteWorker}).
 */
@Slf4j
final class Allocator {
    private final String kind;
    private final RemoteSettings settings;
    private final AttemptCallback callback;

    // session id -> session
    private final Map<String, Slot> slots = new LinkedHashMap<>();
    private final Map<String, Slot> byWorker = new HashMap<>();
    // attempts waiting for credit, in arrival order; unkeyed - those without an affinity key
    private final Map<String, Waiting> waiting = new LinkedHashMap<>();
    private final Map<String, Waiting> unkeyed = new LinkedHashMap<>();
    private final Set<Slot> dirty = new LinkedHashSet<>();
    // failed writes: sessions are closed and their attempts distributed again in flush, after the current pass
    private final List<Slot> failed = new ArrayList<>();
    private final List<Waiting> bounced = new ArrayList<>();
    private boolean stopping;

    Allocator(
        String kind,
        RemoteSettings settings,
        AttemptCallback callback
    ) {
        this.kind = kind;
        this.settings = settings;
        this.callback = callback;
    }

    /**
     * An attempt from the queue; {@code reply} completes when it is sent to a sidecar or rejected.
     */
    void assign(
        Attempt attempt,
        CompletableFuture<Assignment> reply,
        long now
    ) {
        if (stopping) {
            reply.complete(new Assignment.Rejected("node stopping"));
            return;
        }
        final Waiting w = new Waiting(attempt, reply, now);
        enqueue(w);
        widen(w, now);
        flush(now);
    }

    /**
     * A session is opened; a previous session of the same worker id is superseded.
     */
    void join(
        String sessionId,
        String workerId,
        int capacity,
        SessionChannel channel,
        long now
    ) {
        if (stopping) {
            channel.close();
            return;
        }
        final Slot previous = byWorker.get(workerId);
        if (previous != null) {
            drop(previous, "superseded");
        }
        final Slot s = new Slot(sessionId, workerId, capacity, channel);
        slots.put(sessionId, s);
        byWorker.put(workerId, s);
        log.info("Session opened: worker={} kind={} capacity={}", workerId, kind, capacity);
        recomputeCandidates(now);
        dirty.add(s);
        flush(now);
    }

    /**
     * The session is closed by the sidecar side.
     */
    void leave(
        String sessionId,
        String reason,
        long now
    ) {
        final Slot s = slots.get(sessionId);
        if (s != null) {
            drop(s, reason);
            recomputeCandidates(now);
            flush(now);
        }
    }

    /**
     * The sidecar stops: no more attempts for the session; it ends once its attempts in progress are completed.
     */
    void drain(String sessionId, long now) {
        final Slot s = slots.get(sessionId);
        if (s == null || s.draining) {
            return;
        }
        s.draining = true;
        log.info("Session draining: worker={} kind={} inProgress={}", s.workerId, kind, s.inProgress.size());
        if (s.inProgress.isEmpty()) {
            drop(s, "drained");
        }
        recomputeCandidates(now);
        flush(now);
    }

    /**
     * Credit of a session: attempts with session numbers up to {@code limit} may be sent.
     */
    void credited(
        String sessionId,
        long limit,
        long version,
        long now
    ) {
        final Slot s = slots.get(sessionId);
        if (s == null || version <= s.version) {
            return;
        }
        s.limit = limit;
        s.version = version;
        useCredits(s, now);
        dirty.add(s);
        flush(now);
    }

    /**
     * The result of an attempt came from the sidecar: it is no longer in progress there.
     */
    void completed(
        String workerId,
        String taskId,
        int attempt
    ) {
        final Slot s = byWorker.get(workerId);
        if (s != null) {
            final Attempt inProgress = s.inProgress.get(taskId);
            if (inProgress != null && inProgress.number() == attempt) {
                s.inProgress.remove(taskId);
                if (s.draining && s.inProgress.isEmpty()) {
                    drop(s, "drained");
                }
            }
        }
    }

    /**
     * Periodically: widen candidates of waiting attempts, reject those waiting longer than assign-timeout.
     */
    void tick(long now) {
        final long expireBefore = now - settings.assignTimeout().toMillis();
        final List<Waiting> widened = new ArrayList<>();
        final Iterator<Waiting> it = waiting.values().iterator();
        while (it.hasNext()) {
            final Waiting w = it.next();
            if (w.since < expireBefore) {
                it.remove();
                unkeyed.remove(w.taskId());
                unlink(w);
                w.reply.complete(new Assignment.Rejected("no credit from sidecars of kind " + kind + " within "
                    + settings.assignTimeout()));
            } else if (level(w, now) > w.level) {
                widened.add(w);
            }
        }
        widened.stream().filter(w -> waiting.containsKey(w.taskId())).forEach(w -> widen(w, now));
        flush(now);
    }

    /**
     * The node stops: sessions are closed, waiting attempts are rejected.
     */
    void stop() {
        stopping = true;
        final int sessions = slots.size();
        for (Slot s : List.copyOf(slots.values())) {
            drop(s, "node stopping");
        }
        waiting.values().forEach(w -> w.reply.complete(new Assignment.Rejected("node stopping")));
        log.info("Allocator stopped: kind={} sessions={} rejected={}", kind, sessions, waiting.size());
        waiting.clear();
        unkeyed.clear();
        dirty.clear();
    }

    /**
     * Sessions of this kind open on the node.
     */
    int sessions() {
        return slots.size();
    }

    /**
     * After an event: close sessions with failed writes, distribute their attempts again, send changed demand.
     */
    private void flush(long now) {
        do {
            while (!failed.isEmpty()) {
                final List<Slot> gone = List.copyOf(failed);
                failed.clear();
                gone.forEach(s -> drop(s, "send failed"));
                final List<Waiting> again = List.copyOf(bounced);
                bounced.clear();
                again.forEach(this::enqueue);
                recomputeCandidates(now);
            }
            final List<Slot> changed = List.copyOf(dirty);
            dirty.clear();
            changed.forEach(this::sendDemand);
        } while (!failed.isEmpty());
    }

    /**
     * Widening level: 0 - the best by HRW (without a key - two random), 1 - the three best, 2 - all.
     */
    private int level(Waiting w, long now) {
        final long age = now - w.since;
        if (w.key != null) {
            final long wait = settings.affinityWait().toMillis();
            return age >= 2 * wait
                ? 2
                : age >= wait
                    ? 1
                    : 0;
        }
        return age >= settings.spreadWait().toMillis()
            ? 2
            : 0;
    }

    /**
     * Recomputes the candidates of an attempt by its age; if one of them has credit - sends it right away.
     */
    private void widen(Waiting w, long now) {
        unlink(w);
        w.level = level(w, now);
        w.candidates = candidates(w.key, w.level);
        for (int i = 0; i < w.candidates.size(); i++) {
            final Slot s = w.candidates.get(i);
            (i == 0 ? s.primary : s.secondary).add(w);
            dirty.add(s);
        }
        // without a key credit of any sidecar will do
        final Iterable<Slot> withCredit = w.key != null
            ? w.candidates
            : slots.values();
        for (Slot s : withCredit) {
            if (s.usable() > 0) {
                deliver(s, w, now);
                return;
            }
        }
    }

    private List<Slot> candidates(String key, int level) {
        final List<Slot> all = new ArrayList<>(slots.values().stream().filter(Slot::open).toList());
        if (key != null) {
            final int n = switch (level) {
                case 0 -> 1;
                case 1 -> 3;
                default -> all.size();
            };
            all.sort((a, b) -> Long.compareUnsigned(Hrw.score(key, b.workerId), Hrw.score(key, a.workerId)));
            return all.subList(0, Math.min(n, all.size()));
        }
        if (level == 0 && all.size() > 2) {
            Collections.shuffle(all, ThreadLocalRandom.current());
            return all.subList(0, 2);
        }
        return all;
    }

    /**
     * Credit of a session goes to its waiting attempts: first those where it is the best candidate, then those where
     * it is a candidate, then any attempt without a key. What is left is returned with Demand.
     */
    private void useCredits(Slot s, long now) {
        while (s.usable() > 0) {
            Waiting w = first(s.primary);
            if (w == null) {
                w = first(s.secondary);
            }
            if (w == null) {
                w = first(unkeyed.values());
            }
            if (w == null) {
                return;
            }
            deliver(s, w, now);
        }
    }

    private void deliver(
        Slot s,
        Waiting w,
        long now
    ) {
        dequeue(w);
        unlink(w);
        if (!s.channel.attempt(w.attempt)) {
            log.warn("Session send failed: worker={} task={}", s.workerId, w.taskId());
            fail(s);
            bounced.add(w);
            return;
        }
        s.delivered++;
        s.inProgress.put(w.taskId(), w.attempt);
        dirty.add(s);
        log.debug("Attempt sent: worker={} task={} attempt={} waitMs={}",
            s.workerId, w.taskId(), w.attempt.number(), now - w.since);
        w.reply.complete(new Assignment.Accepted());
    }

    /**
     * Sends Demand when the target changed; more credit than attempts - returns the surplus.
     */
    private void sendDemand(Slot s) {
        if (!s.open() || slots.get(s.sessionId) != s) {
            return;
        }
        final long target = s.target();
        final boolean surplus = s.delivered + s.usable() > target;
        if (target == s.sentTarget && !surplus) {
            return;
        }
        if (surplus) {
            // until a new credit version do not send above target: the sidecar may have taken these slots back
            s.returnedAt = s.version;
            s.cap = target;
        }
        s.sentTarget = target;
        if (!s.channel.demand(target, s.version)) {
            fail(s);
        }
    }

    private void fail(Slot s) {
        if (!s.failed) {
            s.failed = true;
            failed.add(s);
        }
    }

    private void enqueue(Waiting w) {
        waiting.put(w.taskId(), w);
        if (w.key == null) {
            unkeyed.put(w.taskId(), w);
        }
    }

    private void dequeue(Waiting w) {
        waiting.remove(w.taskId());
        unkeyed.remove(w.taskId());
    }

    private void unlink(Waiting w) {
        for (Slot s : w.candidates) {
            if (s.primary.remove(w) | s.secondary.remove(w)) {
                dirty.add(s);
            }
        }
    }

    /**
     * The session ends: the channel is closed, its attempts in progress are retried; its candidacies are recomputed
     * by the caller ({@link #recomputeCandidates}).
     */
    private void drop(Slot s, String reason) {
        if (!slots.remove(s.sessionId, s)) {
            return;
        }
        byWorker.remove(s.workerId, s);
        dirty.remove(s);
        s.channel.close();
        log.info("Session closed: worker={} kind={} reason={} inProgress={}", s.workerId, kind, reason, s.inProgress.size());
        for (Attempt attempt : s.inProgress.values()) {
            callback.complete(AttemptResult.retry(attempt, "session of " + s.workerId + " closed: " + reason));
        }
        s.inProgress.clear();
    }

    /**
     * The set of sessions changed: HRW candidates are different.
     */
    private void recomputeCandidates(long now) {
        for (Waiting w : List.copyOf(waiting.values())) {
            if (waiting.containsKey(w.taskId())) {
                widen(w, now);
            }
        }
    }

    private static Waiting first(Iterable<Waiting> items) {
        final Iterator<Waiting> it = items.iterator();
        return it.hasNext()
            ? it.next()
            : null;
    }

    /**
     * An attempt waiting for credit.
     */
    private static final class Waiting {
        final Attempt attempt;
        final CompletableFuture<Assignment> reply;
        final long since;
        final String key;
        List<Slot> candidates = List.of();
        int level;

        Waiting(
            Attempt attempt,
            CompletableFuture<Assignment> reply,
            long since
        ) {
            this.attempt = attempt;
            this.reply = reply;
            this.since = since;
            this.key = attempt.task().meta().get(Task.AFFINITY_KEY);
        }

        String taskId() {
            return attempt.task().id();
        }
    }

    /**
     * Session of a sidecar on this node and its credit.
     */
    private static final class Slot {
        final String sessionId;
        final String workerId;
        final int capacity;
        final SessionChannel channel;
        // waiting attempts where the session is the best candidate, and where it is just a candidate
        final Set<Waiting> primary = new LinkedHashSet<>();
        final Set<Waiting> secondary = new LinkedHashSet<>();
        // task id -> attempt sent in this session, without a result yet
        final Map<String, Attempt> inProgress = new HashMap<>();
        long delivered;
        long limit;
        long version;
        long sentTarget = -1;
        // surplus credit was returned at this version: until it changes, the limit is at most cap
        long returnedAt = -1;
        long cap;
        // a write to the channel failed: the session is closed in flush
        boolean failed;
        // the sidecar stops: no more attempts, the session ends with its last result
        boolean draining;

        Slot(
            String sessionId,
            String workerId,
            int capacity,
            SessionChannel channel
        ) {
            this.sessionId = sessionId;
            this.workerId = workerId;
            this.capacity = capacity;
            this.channel = channel;
        }

        /**
         * Takes attempts: neither failed nor draining.
         */
        boolean open() {
            return !failed && !draining;
        }

        long usable() {
            if (!open()) {
                return 0;
            }
            final long effective = returnedAt == version
                ? Math.min(limit, cap)
                : limit;
            return Math.max(0, effective - delivered);
        }

        /**
         * delivered + demand. Demand up to capacity is exact; above it - in steps capacity * 2^k (rounded down):
         * the sidecar sees which node has the longer queue, and Demand is sent only when the step changes.
         */
        long target() {
            final int want = primary.size() + secondary.size();
            if (want <= capacity) {
                return delivered + want;
            }
            return delivered + (long) capacity * Integer.highestOneBit(want / capacity);
        }
    }
}
