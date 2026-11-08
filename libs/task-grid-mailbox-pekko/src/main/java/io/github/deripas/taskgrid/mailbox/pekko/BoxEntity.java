package io.github.deripas.taskgrid.mailbox.pekko;

import io.github.deripas.taskgrid.mailbox.Box;
import io.github.deripas.taskgrid.mailbox.MailboxSettings;
import io.github.deripas.taskgrid.mailbox.Reply;
import io.github.deripas.taskgrid.mailbox.Reservation;
import io.github.deripas.taskgrid.task.TaskResult;
import org.apache.pekko.Done;
import org.apache.pekko.actor.CoordinatedShutdown;
import org.apache.pekko.actor.typed.ActorRef;
import org.apache.pekko.actor.typed.Behavior;
import org.apache.pekko.actor.typed.javadsl.ActorContext;
import org.apache.pekko.actor.typed.javadsl.Behaviors;
import org.apache.pekko.actor.typed.javadsl.TimerScheduler;
import org.apache.pekko.cluster.sharding.typed.javadsl.ClusterSharding;
import org.apache.pekko.cluster.sharding.typed.javadsl.EntityTypeKey;
import org.apache.pekko.pattern.StatusReply;
import org.apache.pekko.persistence.typed.PersistenceId;
import org.apache.pekko.persistence.typed.RecoveryCompleted;
import org.apache.pekko.persistence.typed.SnapshotAdapter;
import org.apache.pekko.persistence.typed.javadsl.CommandHandler;
import org.apache.pekko.persistence.typed.javadsl.Effect;
import org.apache.pekko.persistence.typed.javadsl.EventHandler;
import org.apache.pekko.persistence.typed.javadsl.EventSourcedBehavior;
import org.apache.pekko.persistence.typed.javadsl.RetentionCriteria;
import org.apache.pekko.persistence.typed.javadsl.SignalHandler;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Set;

/**
 * A box of the mailbox in the cluster: a sharded event-sourced entity per {@link Box}, its requests in the order of
 * their last change. Reservations, results, acknowledgements and expiries are events; the subscription is not
 * persisted - its subscriber ({@link BoxSubscriber}, on any node) subscribes again when the box does not know it.
 * <ul>
 *   <li>Subscription: one subscriber, watched; a newer one gets the box and the older one is told it is superseded.
 *   Results not sent to this subscriber go out within its demand; a new subscription sends again what is not
 *   acknowledged.</li>
 *   <li>Expiry: a timer at the earliest expiry of a request persists {@link Expired}. A box with requests is
 *   remembered by sharding and stays in memory: its requests expire after a restart too.</li>
 *   <li>Passivation: an empty box without a subscriber stops after {@code idle-timeout}; the event that empties it
 *   takes a snapshot of the empty state, so the journal of a long-lived box does not grow.</li>
 * </ul>
 */
final class BoxEntity extends EventSourcedBehavior<BoxEntity.Command, BoxEntity.Event, BoxEntity.State> {
    static final EntityTypeKey<Command> TYPE_KEY = EntityTypeKey.create(Command.class, "task-grid-box");

    /**
     * Messages of the entity, also between nodes: serialized with Jackson (reference.conf).
     */
    sealed interface Command {
    }

    record Reserve(
        String id,
        ActorRef<StatusReply<Reservation>> replyTo
    ) implements Command {
    }

    record Release(
        String id,
        ActorRef<StatusReply<Done>> replyTo
    ) implements Command {
    }

    record Put(
        String id,
        TaskResult result,
        ActorRef<StatusReply<Done>> replyTo
    ) implements Command {
    }

    record Ack(
        List<String> ids,
        ActorRef<StatusReply<Done>> replyTo
    ) implements Command {
    }

    /**
     * The subscriber takes the box with the demand it has not been served yet.
     */
    record Subscribe(
        ActorRef<BoxSubscriber.Message> subscriber,
        long demand
    ) implements Command {
    }

    record Demand(
        ActorRef<BoxSubscriber.Message> subscriber,
        long n
    ) implements Command {
    }

    record Unsubscribe(ActorRef<BoxSubscriber.Message> subscriber) implements Command {
    }

    /**
     * Does the box know the subscriber: answered only when it does not.
     */
    record Heartbeat(ActorRef<BoxSubscriber.Message> subscriber) implements Command {
    }

    record SubscriberGone(ActorRef<BoxSubscriber.Message> subscriber) implements Command {
    }

    record Expire() implements Command {
    }

    /**
     * The box has been empty and without a subscriber for {@code idle-timeout}.
     */
    record Idle() implements Command {
    }

    /**
     * Stop message of sharding: passivation and rebalance.
     */
    record Stop() implements Command {
    }

    /**
     * What happened to the box; the expiry of a request is decided in its event.
     */
    sealed interface Event {
    }

    record Reserved(
        String id,
        long expiresAt
    ) implements Event {
    }

    record Released(String id) implements Event {
    }

    record Stored(
        String id,
        TaskResult result,
        long expiresAt
    ) implements Event {
    }

    record Acked(List<String> ids) implements Event {
    }

    record Expired(List<String> ids) implements Event {
    }

    /**
     * The requests of the box in their order: a snapshot.
     */
    record Snapshot(List<Entry> entries) {
        Snapshot {
            entries = List.copyOf(entries);
        }
    }

    /**
     * A reservation ({@code result} is {@code null}) or a result waiting for its acknowledgement.
     */
    record Entry(
        String id,
        TaskResult result,
        long expiresAt
    ) {
    }

    /**
     * What the boxes of this node share.
     *
     * @param box           limit and ttl of a box
     * @param snapshotEvery events between snapshots
     * @param idleTimeout   an empty box without a subscriber stays this long before it stops
     */
    record Settings(
        MailboxSettings box,
        int snapshotEvery,
        Duration idleTimeout
    ) {
    }

    private static final String EXPIRE = "expire";
    private static final String IDLE = "idle";

    private final ActorContext<Command> context;
    private final TimerScheduler<Command> timers;
    private final ActorRef<ClusterSharding.ShardCommand> shard;
    private final Settings settings;
    private final Box box;
    private ActorRef<BoxSubscriber.Message> subscriber;
    /**
     * Ids of the results sent to the subscriber.
     */
    private final Set<String> sent = new HashSet<>();
    private long demand;

    private BoxEntity(
        ActorContext<Command> context,
        TimerScheduler<Command> timers,
        String entityId,
        ActorRef<ClusterSharding.ShardCommand> shard,
        Settings settings
    ) {
        super(PersistenceId.of(TYPE_KEY.name(), entityId));
        this.context = context;
        this.timers = timers;
        this.shard = shard;
        this.settings = settings;
        this.box = Box.parse(entityId);
    }

    static Behavior<Command> create(
        String entityId,
        ActorRef<ClusterSharding.ShardCommand> shard,
        Settings settings
    ) {
        return Behaviors.setup(context -> Behaviors.withTimers(timers -> new BoxEntity(context, timers, entityId, shard, settings)));
    }

    @Override
    public State emptyState() {
        return new State();
    }

    @Override
    public CommandHandler<Command, Event, State> commandHandler() {
        return newCommandHandlerBuilder()
            .forAnyState()
            .onCommand(Reserve.class, this::reserve)
            .onCommand(Release.class, this::release)
            .onCommand(Put.class, (state, put) -> Effect()
                .persist(new Stored(put.id(), put.result(), expiry()))
                .thenRun(this::deliver)
                .thenRun(this::settle)
                .thenReply(put.replyTo(), s -> StatusReply.ack()))
            .onCommand(Ack.class, this::ack)
            .onCommand(Subscribe.class, this::subscribe)
            .onCommand(Demand.class, this::demand)
            .onCommand(Unsubscribe.class, (state, unsubscribe) -> {
                if (unsubscribe.subscriber().equals(subscriber)) {
                    forgetSubscriber();
                    settle(state);
                }
                return Effect().none();
            })
            .onCommand(Heartbeat.class, (state, heartbeat) -> {
                if (!heartbeat.subscriber().equals(subscriber)) {
                    notTheSubscriber(heartbeat.subscriber());
                }
                return Effect().none();
            })
            .onCommand(SubscriberGone.class, (state, gone) -> {
                if (gone.subscriber().equals(subscriber)) {
                    subscriber = null;
                    settle(state);
                }
                return Effect().none();
            })
            .onCommand(Expire.class, this::expire)
            .onCommand(Idle.class, (state, idle) -> {
                if (state.entries.isEmpty() && subscriber == null) {
                    shard.tell(new ClusterSharding.Passivate<>(context.getSelf()));
                }
                return Effect().none();
            })
            .onCommand(Stop.class, (state, stop) -> {
                final boolean stopping = CoordinatedShutdown.get(context.getSystem()).getShutdownReason().isPresent();
                if (subscriber != null && !stopping) {
                    // moving to another node: the subscriber subscribes there
                    subscriber.tell(new BoxSubscriber.Unknown());
                }
                return Effect().stop();
            })
            .build();
    }

    @Override
    public EventHandler<State, Event> eventHandler() {
        return (state, event) -> {
            state.apply(event);
            return state;
        };
    }

    /**
     * The event that empties the box: the snapshot of the empty state, so that its next recovery reads only that.
     */
    @Override
    public boolean shouldSnapshot(
        State state,
        Event event,
        long sequenceNr
    ) {
        return state.entries.isEmpty();
    }

    @Override
    public RetentionCriteria retentionCriteria() {
        return RetentionCriteria.snapshotEvery(settings.snapshotEvery(), 2).withDeleteEventsOnSnapshot();
    }

    @Override
    public SignalHandler<State> signalHandler() {
        return newSignalHandlerBuilder()
            .onSignal(RecoveryCompleted.instance(), this::settle)
            .build();
    }

    /**
     * The state is mutable: the snapshot store gets an immutable copy, taken on the thread of the entity.
     */
    @Override
    public SnapshotAdapter<State> snapshotAdapter() {
        return new SnapshotAdapter<>() {
            @Override
            public Object toJournal(State state) {
                return new Snapshot(List.copyOf(state.entries.values()));
            }

            @Override
            public State fromJournal(Object from) {
                final State state = new State();
                ((Snapshot) from).entries().forEach(entry -> state.entries.put(entry.id(), entry));
                return state;
            }
        };
    }

    private Effect<Event, State> reserve(State state, Reserve reserve) {
        final long now = now();
        final Entry entry = state.live(reserve.id(), now);
        final Reservation reservation;
        if (entry != null) {
            reservation = entry.result() == null ? Reservation.PENDING : Reservation.DONE;
        } else if (state.live(now) >= settings.box().limit()) {
            reservation = Reservation.FULL;
        } else {
            reservation = Reservation.NEW;
        }
        if (reservation == Reservation.DONE || reservation == Reservation.FULL) {
            return Effect().reply(reserve.replyTo(), StatusReply.success(reservation));
        }
        return Effect().persist(new Reserved(reserve.id(), expiry()))
            .thenRun(this::settle)
            .thenReply(reserve.replyTo(), s -> StatusReply.success(reservation));
    }

    private Effect<Event, State> release(State state, Release release) {
        final Entry entry = state.entries.get(release.id());
        if (entry == null || entry.result() != null) {
            return Effect().reply(release.replyTo(), StatusReply.ack());
        }
        return Effect().persist(new Released(release.id()))
            .thenRun(this::settle)
            .thenReply(release.replyTo(), s -> StatusReply.ack());
    }

    private Effect<Event, State> ack(State state, Ack ack) {
        final List<String> ids = new ArrayList<>();
        for (String id : ack.ids()) {
            final Entry entry = state.entries.get(id);
            if (entry != null && entry.result() != null) {
                ids.add(id);
            }
        }
        if (ids.isEmpty()) {
            return Effect().reply(ack.replyTo(), StatusReply.ack());
        }
        return Effect().persist(new Acked(ids))
            .thenRun(s -> {
                ids.forEach(sent::remove);
                settle(s);
            })
            .thenReply(ack.replyTo(), s -> StatusReply.ack());
    }

    private Effect<Event, State> subscribe(State state, Subscribe subscribe) {
        if (subscriber != null && !subscriber.equals(subscribe.subscriber())) {
            subscriber.tell(new BoxSubscriber.Superseded());
            context.unwatch(subscriber);
        }
        subscriber = subscribe.subscriber();
        context.watchWith(subscriber, new SubscriberGone(subscriber));
        sent.clear();
        demand = subscribe.demand();
        deliver(state);
        settle(state);
        return Effect().none();
    }

    private Effect<Event, State> demand(State state, Demand demand) {
        if (!demand.subscriber().equals(subscriber)) {
            notTheSubscriber(demand.subscriber());
            return Effect().none();
        }
        this.demand = this.demand + demand.n() < 0 ? Long.MAX_VALUE : this.demand + demand.n();
        deliver(state);
        return Effect().none();
    }

    private Effect<Event, State> expire(State state, Expire expire) {
        final List<String> ids = state.expired(now());
        if (ids.isEmpty()) {
            settle(state);
            return Effect().none();
        }
        return Effect().persist(new Expired(ids)).thenRun(s -> {
            ids.forEach(sent::remove);
            settle(s);
        });
    }

    /**
     * Another subscriber has the box - this one is superseded; none - the box lost it (restarted, moved).
     */
    private void notTheSubscriber(ActorRef<BoxSubscriber.Message> other) {
        if (subscriber == null) {
            other.tell(new BoxSubscriber.Unknown());
        } else {
            other.tell(new BoxSubscriber.Superseded());
        }
    }

    private void forgetSubscriber() {
        context.unwatch(subscriber);
        subscriber = null;
    }

    /**
     * Results not sent to the subscriber yet, in the order of the box, within its demand.
     */
    private void deliver(State state) {
        if (subscriber == null) {
            return;
        }
        final long now = now();
        for (Entry entry : state.entries.values()) {
            if (demand == 0) {
                return;
            }
            if (entry.result() != null && entry.expiresAt() > now && sent.add(entry.id())) {
                subscriber.tell(new BoxSubscriber.Deliver(new Reply(entry.id(), entry.result())));
                demand--;
            }
        }
    }

    /**
     * The expiry timer at the earliest expiry; empty and without a subscriber - the idle timer.
     */
    private void settle(State state) {
        final long next = state.nextExpiry();
        if (next == Long.MAX_VALUE) {
            timers.cancel(EXPIRE);
        } else {
            timers.startSingleTimer(EXPIRE, new Expire(), Duration.ofMillis(Math.max(0, next - now())));
        }
        if (!state.entries.isEmpty() || subscriber != null) {
            timers.cancel(IDLE);
        } else if (!timers.isTimerActive(IDLE)) {
            timers.startSingleTimer(IDLE, new Idle(), settings.idleTimeout());
        }
    }

    private long expiry() {
        return now() + settings.box().ttl().toMillis();
    }

    private static long now() {
        return System.currentTimeMillis();
    }

    /**
     * Requests of the box by id, in the order of their last change; mutable, changed only by events.
     */
    static final class State {
        private final LinkedHashMap<String, Entry> entries = new LinkedHashMap<>();

        void apply(Event event) {
            switch (event) {
                case Reserved reserved -> change(new Entry(reserved.id(), null, reserved.expiresAt()));
                case Released released -> entries.remove(released.id());
                case Stored stored -> change(new Entry(stored.id(), stored.result(), stored.expiresAt()));
                case Acked acked -> acked.ids().forEach(entries::remove);
                case Expired expired -> expired.ids().forEach(entries::remove);
            }
        }

        Entry live(String id, long now) {
            final Entry entry = entries.get(id);
            return entry == null || entry.expiresAt() <= now
                ? null
                : entry;
        }

        long live(long now) {
            return entries.values().stream().filter(entry -> entry.expiresAt() > now).count();
        }

        List<String> expired(long now) {
            return entries.values().stream().filter(entry -> entry.expiresAt() <= now).map(Entry::id).toList();
        }

        long nextExpiry() {
            return entries.values().stream().mapToLong(Entry::expiresAt).min().orElse(Long.MAX_VALUE);
        }

        private void change(Entry entry) {
            entries.remove(entry.id());
            entries.put(entry.id(), entry);
        }
    }
}
