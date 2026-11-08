package io.github.deripas.taskgrid.mailbox.memory;

import io.github.deripas.taskgrid.mailbox.Box;
import io.github.deripas.taskgrid.mailbox.Mailbox;
import io.github.deripas.taskgrid.mailbox.MailboxSettings;
import io.github.deripas.taskgrid.mailbox.Reply;
import io.github.deripas.taskgrid.mailbox.Reservation;
import io.github.deripas.taskgrid.mailbox.SupersededException;
import io.github.deripas.taskgrid.task.RequestId;
import io.github.deripas.taskgrid.task.TaskResult;

import java.time.Clock;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Flow;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Mailboxes of a single node, in memory: a consumer reaches its box only on this node, and the boxes are lost when
 * the process stops. Expired requests are evicted on the next call, without a background thread; an empty box without
 * a subscription is forgotten.
 * <p>
 * Signals of a subscription come from the thread that caused them (a put, an ack, a request), one at a time.
 */
public final class MemoryMailbox implements Mailbox {
    private final Object lock = new Object();
    private final MailboxSettings settings;
    private final Clock clock;
    private final Map<Box, BoxState> boxes = new HashMap<>();
    /**
     * Requests of all boxes in the order of their last change, which is also the order of expiry.
     */
    private final LinkedHashMap<Key, Entry> entries = new LinkedHashMap<>();

    public MemoryMailbox(MailboxSettings settings, Clock clock) {
        this.settings = settings;
        this.clock = clock;
    }

    @Override
    public CompletionStage<Reservation> reserve(RequestId request) {
        final Box box = Box.of(request);
        synchronized (lock) {
            final Instant now = clock.instant();
            evictExpired(now);
            final BoxState state = boxes.get(box);
            final Entry entry = state == null ? null : state.entries.get(request.id());
            final Reservation reservation;
            if (entry != null) {
                reservation = entry.result() == null ? Reservation.PENDING : Reservation.DONE;
            } else if (state != null && state.entries.size() >= settings.limit()) {
                reservation = Reservation.FULL;
            } else {
                reservation = Reservation.NEW;
            }
            if (reservation == Reservation.NEW || reservation == Reservation.PENDING) {
                change(box, request.id(), new Entry(null, now.plus(settings.ttl())));
            }
            return CompletableFuture.completedFuture(reservation);
        }
    }

    @Override
    public CompletionStage<Void> release(RequestId request) {
        final Box box = Box.of(request);
        synchronized (lock) {
            evictExpired(clock.instant());
            final BoxState state = boxes.get(box);
            if (state != null) {
                final Entry entry = state.entries.get(request.id());
                if (entry != null && entry.result() == null) {
                    remove(box, state, request.id());
                }
            }
        }
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public CompletionStage<Void> put(RequestId request, TaskResult result) {
        final Subscription subscription;
        synchronized (lock) {
            final Instant now = clock.instant();
            evictExpired(now);
            subscription = change(Box.of(request), request.id(), new Entry(result, now.plus(settings.ttl()))).subscription;
        }
        if (subscription != null) {
            subscription.drain();
        }
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public void subscribe(Box box, Flow.Subscriber<? super Reply> subscriber) {
        final Subscription subscription = new Subscription(box, subscriber);
        final Subscription older;
        synchronized (lock) {
            evictExpired(clock.instant());
            final BoxState state = boxes.computeIfAbsent(box, b -> new BoxState());
            older = state.subscription;
            state.subscription = subscription;
        }
        if (older != null) {
            older.fail(new SupersededException(box));
        }
        subscriber.onSubscribe(subscription);
        subscription.start();
    }

    @Override
    public CompletionStage<Void> ack(Box box, Collection<String> ids) {
        synchronized (lock) {
            evictExpired(clock.instant());
            final BoxState state = boxes.get(box);
            if (state != null) {
                for (String id : ids) {
                    final Entry entry = state.entries.get(id);
                    if (entry != null && entry.result() != null) {
                        remove(box, state, id);
                    }
                }
            }
        }
        return CompletableFuture.completedFuture(null);
    }

    /**
     * Puts the request at the end of both orders: of its box (delivery) and of all tasks (expiry).
     */
    private BoxState change(
        Box box,
        String id,
        Entry entry
    ) {
        final BoxState state = boxes.computeIfAbsent(box, b -> new BoxState());
        state.entries.remove(id);
        state.entries.put(id, entry);
        final Key key = new Key(box, id);
        entries.remove(key);
        entries.put(key, entry);
        return state;
    }

    private void remove(
        Box box,
        BoxState state,
        String id
    ) {
        state.entries.remove(id);
        entries.remove(new Key(box, id));
        if (state.subscription != null) {
            state.subscription.sent.remove(id);
        }
        forgetIfIdle(box, state);
    }

    private void evictExpired(Instant now) {
        final Iterator<Map.Entry<Key, Entry>> it = entries.entrySet().iterator();
        while (it.hasNext()) {
            final Map.Entry<Key, Entry> next = it.next();
            if (next.getValue().expiresAt().isAfter(now)) {
                return;
            }
            it.remove();
            final Key key = next.getKey();
            final BoxState state = boxes.get(key.box());
            state.entries.remove(key.id());
            if (state.subscription != null) {
                state.subscription.sent.remove(key.id());
            }
            forgetIfIdle(key.box(), state);
        }
    }

    private void forgetIfIdle(Box box, BoxState state) {
        if (state.entries.isEmpty() && state.subscription == null) {
            boxes.remove(box);
        }
    }

    /**
     * Requests of a box by their ids, in the order of their last change, and its subscription; guarded by the lock.
     */
    private static final class BoxState {
        private final LinkedHashMap<String, Entry> entries = new LinkedHashMap<>();
        private Subscription subscription;
    }

    /**
     * A reservation ({@code result} is {@code null}) or a result waiting for its acknowledgement.
     */
    private record Entry(
        TaskResult result,
        Instant expiresAt
    ) {
    }

    private record Key(
        Box box,
        String id
    ) {
    }

    /**
     * Sends the replies of its box the subscriber has not got yet, within its demand. Its state is guarded by the
     * lock of the mailbox; signals go out of the lock, one drain at a time ({@code wip}).
     */
    private final class Subscription implements Flow.Subscription {
        private final Box box;
        private final Flow.Subscriber<? super Reply> subscriber;
        private final AtomicInteger wip = new AtomicInteger();
        /**
         * Ids of the replies sent by this subscription; the next subscription sends them again unless acknowledged.
         */
        private final Set<String> sent = new HashSet<>();
        private long demand;
        private Throwable error;
        private boolean terminated;
        private volatile boolean started;

        private Subscription(Box box, Flow.Subscriber<? super Reply> subscriber) {
            this.box = box;
            this.subscriber = subscriber;
        }

        @Override
        public void request(long n) {
            if (n <= 0) {
                synchronized (lock) {
                    detach();
                }
                fail(new IllegalArgumentException("request must be positive: " + n));
                return;
            }
            synchronized (lock) {
                demand = demand + n < 0 ? Long.MAX_VALUE : demand + n;
            }
            drain();
        }

        @Override
        public void cancel() {
            synchronized (lock) {
                terminated = true;
                detach();
            }
        }

        private void start() {
            started = true;
            drain();
        }

        private void fail(Throwable cause) {
            synchronized (lock) {
                if (error == null) {
                    error = cause;
                }
            }
            drain();
        }

        private void drain() {
            if (!started || wip.getAndIncrement() != 0) {
                return;
            }
            int missed = 1;
            while (missed != 0) {
                final List<Reply> batch = new ArrayList<>();
                Throwable failure = null;
                synchronized (lock) {
                    if (!terminated && error != null) {
                        failure = error;
                        terminated = true;
                    } else if (!terminated) {
                        take(batch);
                    }
                }
                batch.forEach(subscriber::onNext);
                if (failure != null) {
                    subscriber.onError(failure);
                }
                missed = wip.addAndGet(-missed);
            }
        }

        /**
         * Replies not sent yet, in the order of the box, within the demand; under the lock.
         */
        private void take(List<Reply> batch) {
            final BoxState state = boxes.get(box);
            if (state == null || state.subscription != this) {
                return;
            }
            for (Map.Entry<String, Entry> next : state.entries.entrySet()) {
                if (demand == 0) {
                    return;
                }
                final TaskResult result = next.getValue().result();
                if (result != null && sent.add(next.getKey())) {
                    batch.add(new Reply(next.getKey(), result));
                    demand--;
                }
            }
        }

        /**
         * Under the lock.
         */
        private void detach() {
            final BoxState state = boxes.get(box);
            if (state != null && state.subscription == this) {
                state.subscription = null;
                forgetIfIdle(box, state);
            }
        }
    }
}
