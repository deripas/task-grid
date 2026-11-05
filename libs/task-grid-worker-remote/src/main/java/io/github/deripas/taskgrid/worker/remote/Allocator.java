package io.github.deripas.taskgrid.worker.remote;

import io.github.deripas.taskgrid.task.Attempt;
import io.github.deripas.taskgrid.task.AttemptResult;
import io.github.deripas.taskgrid.worker.Assignment;
import io.github.deripas.taskgrid.worker.AttemptCallback;
import io.github.deripas.taskgrid.worker.Worker;
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
 *   <li>With an affinity key ({@link Worker#AFFINITY_KEY}): the candidate is the best sidecar by HRW(key, worker id);
 *   no credit within affinity-wait - the three best, within twice as long - all.</li>
 *   <li>Without one: two random sidecars, after spread-wait - all; credit of any sidecar takes such an attempt.</li>
 *   <li>Credit without a matching attempt is returned (Demand with target below limit).</li>
 *   <li>Attempts in progress belong to the sidecar process (worker id + instance id), not to its session: a session
 *   is only the channel to send attempts and receive credit. A new session of the same process supersedes
 *   the previous one as the channel; the attempts stay in progress, their results may come through any session
 *   or any node.</li>
 *   <li>A new instance id under the same worker id - the process was restarted and lost its attempts: they are
 *   reassigned at once. Reassigned - put back to wait for credit of other sidecars with the same attempt number,
 *   the queue does not see it.</li>
 *   <li>The session of a process is lost (broken, not drained): its attempts wait rejoin-timeout for the process to
 *   reconnect, then they are reassigned. A process lost while draining is not waited for.</li>
 *   <li>A draining session gets no more attempts and ends when its attempts in progress are completed.</li>
 *   <li>An attempt without credit within assign-timeout is a technical failure: accepted and reported as RETRY,
 *   the queue counts the attempt and hands it out again (the pool may be scaling up).</li>
 *   <li>A failed write to a session channel closes the session as lost; the attempt is distributed again.</li>
 *   <li>The node stops: sessions are closed, attempts in progress are left to their sidecars (the results go to
 *   another node), waiting attempts are reported as RETRY.</li>
 * </ul>
 * Pure state machine: no threads, time is a parameter. Methods are called sequentially, one thread at a time;
 * events and the periodic {@link #tick} are delivered by the engine ({@link RemoteWorker}).
 */
@Slf4j
final class Allocator {
    private static final String NODE_STOPPING = "node stopping";

    private final String kind;
    private final RemoteSettings settings;
    private final AttemptCallback callback;

    // session id -> session
    private final Map<String, Slot> slots = new LinkedHashMap<>();
    // worker id -> sidecar process with attempts in progress or an open session
    private final Map<String, Member> members = new HashMap<>();
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
     * An attempt from the queue; {@code reply} completes when it is sent to a sidecar or accepted for a retry.
     */
    void assign(
        Attempt attempt,
        CompletableFuture<Assignment> reply,
        long now
    ) {
        if (stopping) {
            reply.complete(new Assignment.Accepted());
            callback.complete(AttemptResult.retry(attempt, NODE_STOPPING));
            return;
        }
        final Waiting w = new Waiting(attempt, reply, now);
        enqueue(w);
        widen(w, now);
        flush(now);
    }

    /**
     * A session is opened. The same process (worker id and instance id) - the previous session is superseded as
     * the channel, the attempts in progress stay; another instance id - the attempts of the previous process are
     * reassigned.
     */
    void join(
        String sessionId,
        SidecarHello hello,
        SessionChannel channel,
        long now
    ) {
        if (stopping) {
            channel.close();
            return;
        }
        Member m = members.get(hello.workerId());
        if (m != null && !m.instanceId.equals(hello.instanceId())) {
            if (m.session != null) {
                close(m.session, "process restarted");
            }
            reassign(m, "process of " + m.workerId + " restarted", now);
            m = null;
        }
        if (m == null) {
            m = new Member(hello.workerId(), hello.instanceId());
            members.put(m.workerId, m);
        } else if (m.session != null) {
            close(m.session, "superseded");
        }
        final Slot s = new Slot(sessionId, m, hello.capacity(), channel);
        slots.put(sessionId, s);
        m.session = s;
        log.info("Session opened: worker={} instance={} kind={} capacity={} inProgress={}",
            m.workerId, m.instanceId, kind, hello.capacity(), m.inProgress.size());
        recomputeCandidates(now);
        dirty.add(s);
        flush(now);
    }

    /**
     * The session is closed by the sidecar side (broken, cancelled): the process is lost until it reconnects.
     */
    void leave(
        String sessionId,
        String reason,
        long now
    ) {
        final Slot s = slots.get(sessionId);
        if (s != null) {
            close(s, reason);
            lost(s.member, now);
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
        s.member.draining = true;
        log.info("Session draining: worker={} kind={} inProgress={}", s.workerId(), kind, s.member.inProgress.size());
        if (s.member.inProgress.isEmpty()) {
            close(s, "drained");
            members.remove(s.member.workerId, s.member);
        }
        recomputeCandidates(now);
        flush(now);
    }

    /**
     * Share of the capacity of the sidecar this node counts in its load: the sidecar splits its capacity among
     * the nodes it is connected to.
     */
    void shared(String sessionId, double share) {
        final Slot s = slots.get(sessionId);
        if (s != null) {
            s.share = share;
        }
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
     * The result of an attempt came from the sidecar, through any of its sessions: it is no longer in progress there.
     */
    void completed(
        String workerId,
        String taskId,
        int attempt
    ) {
        final Member m = members.get(workerId);
        if (m == null) {
            return;
        }
        final Attempt inProgress = m.inProgress.get(taskId);
        if (inProgress == null || inProgress.number() != attempt) {
            return;
        }
        m.inProgress.remove(taskId);
        if (!m.inProgress.isEmpty()) {
            return;
        }
        if (m.session == null) {
            members.remove(workerId, m);
        } else if (m.draining) {
            close(m.session, "drained");
            members.remove(workerId, m);
        }
    }

    /**
     * Periodically: reassign attempts of processes lost longer than rejoin-timeout, widen candidates of waiting
     * attempts, retry those waiting longer than assign-timeout.
     */
    void tick(long now) {
        final long lostBefore = now - settings.rejoinTimeout().toMillis();
        for (Member m : List.copyOf(members.values())) {
            if (m.session == null && m.lostAt < lostBefore) {
                reassign(m, "no session of " + m.workerId + " within " + settings.rejoinTimeout(), now);
            }
        }
        final long expireBefore = now - settings.assignTimeout().toMillis();
        final List<Waiting> widened = new ArrayList<>();
        final Iterator<Waiting> it = waiting.values().iterator();
        while (it.hasNext()) {
            final Waiting w = it.next();
            if (w.since < expireBefore) {
                it.remove();
                unkeyed.remove(w.taskId());
                unlink(w);
                w.reply.complete(new Assignment.Accepted());
                callback.complete(AttemptResult.retry(w.attempt, "no credit from sidecars of kind " + kind + " within "
                    + settings.assignTimeout()));
            } else if (level(w, now) > w.level) {
                widened.add(w);
            }
        }
        widened.stream().filter(w -> waiting.containsKey(w.taskId())).forEach(w -> widen(w, now));
        flush(now);
    }

    /**
     * The node stops: sessions are closed; attempts in progress are left to their sidecars - the results go to
     * another node, and the queue retries an attempt without a result after its timeout; waiting attempts are
     * reported as RETRY.
     */
    void stop() {
        stopping = true;
        final int sessions = slots.size();
        for (Slot s : List.copyOf(slots.values())) {
            close(s, NODE_STOPPING);
        }
        members.clear();
        for (Waiting w : waiting.values()) {
            w.reply.complete(new Assignment.Accepted());
            callback.complete(AttemptResult.retry(w.attempt, NODE_STOPPING));
        }
        log.info("Allocator stopped: kind={} sessions={} retried={}", kind, sessions, waiting.size());
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
     * Attempts waiting for credit, attempts in progress of the sidecars of this kind (also of lost ones, until they
     * rejoin or their attempts are reassigned) and the shares of their capacity on this node.
     */
    Load load() {
        int busy = 0;
        for (Member m : members.values()) {
            busy += m.inProgress.size();
        }
        double capacity = 0;
        for (Slot s : slots.values()) {
            capacity += s.share;
        }
        return new Load(waiting.size(), busy, capacity);
    }

    /**
     * After an event: close sessions with failed writes, distribute their attempts again, send changed demand.
     */
    private void flush(long now) {
        do {
            while (!failed.isEmpty()) {
                final List<Slot> gone = List.copyOf(failed);
                failed.clear();
                for (Slot s : gone) {
                    if (close(s, "send failed")) {
                        lost(s.member, now);
                    }
                }
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
            all.sort((a, b) -> Long.compareUnsigned(Hrw.score(key, b.workerId()), Hrw.score(key, a.workerId())));
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
            log.warn("Session send failed: worker={} task={}", s.workerId(), w.taskId());
            fail(s);
            bounced.add(w);
            return;
        }
        s.delivered++;
        s.member.inProgress.put(w.taskId(), w.attempt);
        dirty.add(s);
        log.debug("Attempt sent: worker={} task={} attempt={} waitMs={}",
            s.workerId(), w.taskId(), w.attempt.number(), now - w.since);
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
     * The session ends as a channel: it is closed, the attempts in progress stay with its process; candidacies are
     * recomputed by the caller ({@link #recomputeCandidates}). {@code false} - already closed.
     */
    private boolean close(Slot s, String reason) {
        if (!slots.remove(s.sessionId, s)) {
            return false;
        }
        dirty.remove(s);
        s.channel.close();
        if (s.member.session == s) {
            s.member.session = null;
        }
        log.info("Session closed: worker={} kind={} reason={} inProgress={}", s.workerId(), kind, reason, s.member.inProgress.size());
        return true;
    }

    /**
     * The process has no session any more: nothing in progress - forgotten; draining - its attempts are reassigned
     * at once (it is leaving); otherwise they wait rejoin-timeout for it to reconnect.
     */
    private void lost(Member m, long now) {
        if (m.session != null || members.get(m.workerId) != m) {
            return;
        }
        if (m.inProgress.isEmpty()) {
            members.remove(m.workerId, m);
        } else if (m.draining) {
            reassign(m, "process of " + m.workerId + " left while draining", now);
        } else {
            m.lostAt = now;
        }
    }

    /**
     * The process is forgotten; its attempts in progress wait for credit of other sidecars again, with the same
     * attempt number. An attempt of the task already waiting (a newer one from the queue) wins.
     */
    private void reassign(
        Member m,
        String reason,
        long now
    ) {
        members.remove(m.workerId, m);
        if (m.inProgress.isEmpty()) {
            return;
        }
        log.info("Attempts reassigned: kind={} reason={} attempts={}", kind, reason, m.inProgress.size());
        for (Attempt attempt : m.inProgress.values()) {
            if (!waiting.containsKey(attempt.task().id())) {
                final Waiting w = new Waiting(attempt, new CompletableFuture<>(), now);
                enqueue(w);
                widen(w, now);
            }
        }
        m.inProgress.clear();
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
            this.key = attempt.task().meta().get(Worker.AFFINITY_KEY);
        }

        String taskId() {
            return attempt.task().id();
        }
    }

    /**
     * Sidecar process: its attempts in progress outlive its sessions.
     */
    private static final class Member {
        final String workerId;
        final String instanceId;
        // task id -> attempt sent to the process, without a result yet
        final Map<String, Attempt> inProgress = new HashMap<>();
        // the current session; null - lost (since lostAt), waits to reconnect
        Slot session;
        long lostAt;
        // the process stops: it opens no new sessions
        boolean draining;

        Member(String workerId, String instanceId) {
            this.workerId = workerId;
            this.instanceId = instanceId;
        }
    }

    /**
     * Session of a sidecar process on this node and its credit.
     */
    private static final class Slot {
        final String sessionId;
        final Member member;
        final int capacity;
        final SessionChannel channel;
        // waiting attempts where the session is the best candidate, and where it is just a candidate
        final Set<Waiting> primary = new LinkedHashSet<>();
        final Set<Waiting> secondary = new LinkedHashSet<>();
        // share of the capacity counted in the load of this node
        double share;
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
            Member member,
            int capacity,
            SessionChannel channel
        ) {
            this.sessionId = sessionId;
            this.member = member;
            this.capacity = capacity;
            this.channel = channel;
            this.share = capacity;
        }

        String workerId() {
            return member.workerId;
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
