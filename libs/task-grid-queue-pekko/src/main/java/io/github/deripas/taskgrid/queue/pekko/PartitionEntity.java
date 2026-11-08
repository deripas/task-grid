package io.github.deripas.taskgrid.queue.pekko;

import io.github.deripas.taskgrid.queue.DelayPolicy;
import io.github.deripas.taskgrid.queue.QueueSettings;
import io.github.deripas.taskgrid.queue.TaskQueue;
import io.github.deripas.taskgrid.queue.partition.Partition;
import io.github.deripas.taskgrid.queue.partition.PartitionEvent;
import io.github.deripas.taskgrid.queue.partition.PartitionSnapshot;
import io.github.deripas.taskgrid.queue.partition.Step;
import io.github.deripas.taskgrid.task.AttemptResult;
import io.github.deripas.taskgrid.task.Request;
import io.github.deripas.taskgrid.task.Task;
import lombok.extern.slf4j.Slf4j;
import org.apache.pekko.Done;
import org.apache.pekko.actor.typed.ActorRef;
import org.apache.pekko.actor.CoordinatedShutdown;
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
import org.apache.pekko.persistence.typed.javadsl.EffectBuilder;
import org.apache.pekko.persistence.typed.javadsl.EventHandler;
import org.apache.pekko.persistence.typed.javadsl.EventSourcedBehavior;
import org.apache.pekko.persistence.typed.javadsl.RetentionCriteria;
import org.apache.pekko.persistence.typed.javadsl.SignalHandler;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;

/**
 * A partition of the queue in the cluster: a sharded event-sourced entity per partition key, running the
 * {@link Partition} state machine. Events are persisted before anyone hears of them; then the attempts go to the
 * receivers of this node.
 * <ul>
 *   <li>A final result is not persisted: the entity delivers it to the receivers of this node first, with the other
 *   commands stashed, and persists the end of the task only once all of them succeeded. A delivery that failed
 *   decides again: the result of a worker fails its {@code Complete} (the worker repeats it), the result of a deadline
 *   is decided again by a tick after {@code redeliver-after}. A receiver that is down holds its partitions.</li>
 *   <li>Timers: the deadline of the attempt in progress and the end of the pause ({@link Partition#nextDeadline}).</li>
 *   <li>Recovery: from the last snapshot and the events after it; then a tick (deadlines that passed while the entity
 *   was down). An attempt in progress is not handed out again: its result may still come, on any node; otherwise its
 *   deadline retries it.</li>
 *   <li>Passivation: an empty entity (nothing waiting or in progress) stays for {@code idle-timeout}, then
 *   stops itself if still empty and its pause is over. A key whose tasks come seldom but more often than that is not
 *   recovered from the journal for each of them, and its pause is known from memory; once it is gone, the pause is
 *   surely over and the next task may start at once. The value is a matter of cost, not of politeness: the entity
 *   never stops before its pause ends. Before it stops it persists {@link Rested} with a snapshot of the empty state:
 *   the next recovery of the key reads the snapshot only, the journal of a long-lived key does not grow.
 *   A command may reach it after it asked to: if it has work when the stop comes, it sends itself a tick through
 *   sharding before stopping - the shard holds the tick and starts the entity again. A stop it did not ask for
 *   (rebalance, the node leaving) needs no tick: the entity is remembered and starts on its new node.</li>
 * </ul>
 */
@Slf4j
final class PartitionEntity extends EventSourcedBehavior<PartitionEntity.Command, Object, Partition> {
    static final EntityTypeKey<Command> TYPE_KEY = EntityTypeKey.create(Command.class, "task-grid-partition");

    /**
     * Messages of the entity, also between nodes: serialized with Jackson (reference.conf).
     */
    sealed interface Command {
    }

    /**
     * Replies {@link StatusReply#ack()} once the request is persisted (or was there already), an error if the id of
     * the task is taken by another payload or the request is there with another task.
     */
    record Submit(
        Task task,
        Request request,
        ActorRef<StatusReply<Done>> replyTo
    ) implements Command {
    }

    /**
     * Replies once the result is taken: a final one - delivered and its end persisted, an error if the delivery
     * failed; at once if the attempt is not in progress (it is ignored).
     */
    record Complete(
        AttemptResult result,
        ActorRef<StatusReply<Done>> replyTo
    ) implements Command {
    }

    record Tick() implements Command {
    }

    /**
     * The final result of the {@link #pending} step reached all the receivers.
     */
    record Delivered() implements Command {
    }

    record NotDelivered(String error) implements Command {
    }

    /**
     * The entity has been empty for {@code idle-timeout}.
     */
    record Idle() implements Command {
    }

    /**
     * Event of the entity, not of the partition: it is empty and its pause is over, about to passivate. Triggers the
     * snapshot of the empty state (events before it are deleted by the retention).
     */
    record Rested() {
    }

    /**
     * Stop message of sharding: passivation and rebalance.
     */
    record Stop() implements Command {
    }

    /**
     * What the entities of this node share.
     *
     * @param redeliverAfter  pause before deciding a result of a deadline again after a failed delivery
     * @param deliveryTimeout a delivery of a final result not done this long has failed
     * @param snapshotEvery   events between snapshots
     * @param idleTimeout     an empty entity stays this long before it passivates
     */
    record Settings(
        QueueSettings queue,
        DelayPolicy delay,
        Duration redeliverAfter,
        Duration deliveryTimeout,
        int snapshotEvery,
        Duration idleTimeout
    ) {
    }

    private static final String TICK = "tick";
    private static final String IDLE = "idle";
    /**
     * A tick after a failed delivery of the result of a deadline; apart from {@link #TICK}, which follows the deadline.
     */
    private static final String REDECIDE = "redecide";
    private static final String KEY_PREFIX = "k:";
    private static final String TASK_PREFIX = "t:";

    private final ActorContext<Command> context;
    private final TimerScheduler<Command> timers;
    private final ActorRef<ClusterSharding.ShardCommand> shard;
    private final String entityId;
    private final Settings settings;
    private final Receivers receivers;
    /**
     * Partition key, {@code null} - an independent task.
     */
    private final String key;
    /**
     * The step whose final result is being delivered: its events are persisted after the delivery, other commands
     * wait in the stash; {@code null} - none.
     */
    private Pending pending;
    /**
     * Passivation is requested: the next stop is the answer to it.
     */
    private boolean passivating;
    /**
     * Partition events after the last {@link Rested}, also replayed ones: an empty partition needs a new snapshot.
     */
    private boolean changed;

    private PartitionEntity(
        ActorContext<Command> context,
        TimerScheduler<Command> timers,
        String entityId,
        ActorRef<ClusterSharding.ShardCommand> shard,
        Settings settings,
        Receivers receivers
    ) {
        super(PersistenceId.of(TYPE_KEY.name(), entityId));
        this.context = context;
        this.timers = timers;
        this.shard = shard;
        this.entityId = entityId;
        this.settings = settings;
        this.receivers = receivers;
        this.key = keyOf(entityId);
    }

    static Behavior<Command> create(
        String entityId,
        ActorRef<ClusterSharding.ShardCommand> shard,
        Settings settings,
        Receivers receivers
    ) {
        return Behaviors.setup(context -> Behaviors.withTimers(timers ->
            new PartitionEntity(context, timers, entityId, shard, settings, receivers)));
    }

    /**
     * Entity of a task: its partition key, or the task itself when it has none.
     */
    static String entityId(String taskId, Map<String, String> meta) {
        final String partitionKey = meta.get(TaskQueue.PARTITION_KEY);
        return partitionKey != null
            ? KEY_PREFIX + partitionKey
            : TASK_PREFIX + taskId;
    }

    @Override
    public Partition emptyState() {
        return new Partition(key, settings.queue(), settings.delay());
    }

    @Override
    public CommandHandler<Command, Object, Partition> commandHandler() {
        return newCommandHandlerBuilder()
            .forAnyState()
            .onCommand(Delivered.class, this::delivered)
            .onCommand(NotDelivered.class, this::notDelivered)
            .onCommand(Stop.class, this::stop)
            .onCommand(c -> pending != null, (state, c) -> Effect().stash())
            .onCommand(Submit.class, this::submit)
            .onCommand(Complete.class, (state, c) -> run(state.completed(c.result(), now()), c.replyTo()))
            .onCommand(Tick.class, (state, t) -> run(state.tick(now()), null))
            .onCommand(Idle.class, this::idle)
            .build();
    }

    @Override
    public EventHandler<Partition, Object> eventHandler() {
        return (state, event) -> {
            if (event instanceof PartitionEvent change) {
                state.apply(change);
                changed = true;
            } else {
                changed = false;
            }
            return state;
        };
    }

    /**
     * {@link Rested}: the snapshot of the empty partition, so that its next recovery reads only that.
     */
    @Override
    public boolean shouldSnapshot(
        Partition state,
        Object event,
        long sequenceNr
    ) {
        return event instanceof Rested;
    }

    @Override
    public SignalHandler<Partition> signalHandler() {
        return newSignalHandlerBuilder()
            .onSignal(RecoveryCompleted.instance(), state -> {
                if (state.isEmpty()) {
                    settle(state);
                } else {
                    context.getSelf().tell(new Tick());
                }
            })
            .build();
    }

    @Override
    public RetentionCriteria retentionCriteria() {
        return RetentionCriteria.snapshotEvery(settings.snapshotEvery(), 2).withDeleteEventsOnSnapshot();
    }

    /**
     * The state is mutable: the snapshot store gets an immutable copy, taken on the thread of the entity.
     */
    @Override
    public SnapshotAdapter<Partition> snapshotAdapter() {
        return new SnapshotAdapter<>() {
            @Override
            public Object toJournal(Partition state) {
                return state.snapshot();
            }

            @Override
            public Partition fromJournal(Object from) {
                return Partition.restore(key, settings.queue(), settings.delay(), (PartitionSnapshot) from);
            }
        };
    }

    private Effect<Object, Partition> submit(Partition state, Submit submit) {
        final List<PartitionEvent> events;
        try {
            events = state.submit(submit.task(), submit.request(), now());
        } catch (IllegalArgumentException e) {
            return Effect().reply(submit.replyTo(), StatusReply.error(e.getMessage()));
        }
        return persist(events).thenReply(submit.replyTo(), s -> StatusReply.ack());
    }

    /**
     * Empty for the idle timeout: passivate, unless work came or the pause still lasts (then once it ends).
     */
    private Effect<Object, Partition> idle(Partition state, Idle idle) {
        final long now = now();
        if (!state.isEmpty() || passivating) {
            return Effect().none();
        }
        if (!state.isIdle(now)) {
            timers.startSingleTimer(IDLE, new Idle(), Duration.ofMillis(state.nextDeadline() - now));
            return Effect().none();
        }
        passivating = true;
        if (!changed) {
            passivate();
            return Effect().none();
        }
        return Effect().persist(new Rested()).thenRun(rested -> passivate());
    }

    private void passivate() {
        shard.tell(new ClusterSharding.Passivate<>(context.getSelf()));
    }

    /**
     * Passivation or rebalance. Work that came after the request to passivate is not dropped: a tick through sharding
     * starts the entity again (here or on its new node).
     */
    private Effect<Object, Partition> stop(Partition state, Stop stop) {
        final boolean stopping = CoordinatedShutdown.get(context.getSystem()).getShutdownReason().isPresent();
        if (passivating && !stopping && !state.isIdle(now())) {
            log.debug("Work came while passivating, starting again: key={}", key);
            ClusterSharding.get(context.getSystem()).entityRefFor(TYPE_KEY, entityId).tell(new Tick());
        }
        return Effect().stop();
    }

    /**
     * A step without a final result is persisted at once; with one - the result goes to the receivers first.
     *
     * @param replyTo of the {@link Complete} that decided the step, {@code null} - of a tick
     */
    private Effect<Object, Partition> run(Step step, ActorRef<StatusReply<Done>> replyTo) {
        if (step.finished() == null) {
            final EffectBuilder<Object, Partition> effect = persist(step.events());
            return replyTo == null
                ? effect
                : effect.thenReply(replyTo, s -> StatusReply.ack());
        }
        pending = new Pending(step, replyTo);
        final Step.Finished finished = step.finished();
        final CompletionStage<Void> delivery = receivers.deliver(finished.result(), finished.requests())
            .toCompletableFuture()
            .orTimeout(settings.deliveryTimeout().toMillis(), TimeUnit.MILLISECONDS);
        context.pipeToSelf(delivery, (ignored, error) -> error == null
            ? new Delivered()
            : new NotDelivered(String.valueOf(error)));
        timers.cancel(IDLE);
        return Effect().none();
    }

    private Effect<Object, Partition> delivered(Partition state, Delivered delivered) {
        final Pending done = pending;
        if (done == null) {
            return Effect().none();
        }
        pending = null;
        final EffectBuilder<Object, Partition> persisted = persist(done.step().events());
        final EffectBuilder<Object, Partition> effect = done.replyTo() == null
            ? persisted
            : persisted.thenRun(s -> done.replyTo().tell(StatusReply.ack()));
        return effect.thenUnstashAll();
    }

    private Effect<Object, Partition> notDelivered(Partition state, NotDelivered failed) {
        final Pending done = pending;
        if (done == null) {
            return Effect().none();
        }
        pending = null;
        final String taskId = done.step().finished().result().taskId();
        if (done.replyTo() != null) {
            log.warn("Result not delivered, the worker repeats it: key={} task={} error={}", key, taskId, failed.error());
            done.replyTo().tell(StatusReply.error("result not delivered: " + failed.error()));
        } else {
            log.warn("Result not delivered, decided again in {}: key={} task={} error={}",
                settings.redeliverAfter(), key, taskId, failed.error());
            timers.startSingleTimer(REDECIDE, new Tick(), settings.redeliverAfter());
        }
        return Effect().unstashAll();
    }

    /**
     * Persists the events, then carries out their effects and sets the timer; no events - only the timer.
     */
    private EffectBuilder<Object, Partition> persist(List<PartitionEvent> events) {
        if (events.isEmpty()) {
            return Effect().none().thenRun(this::settle);
        }
        return Effect().persist(List.<Object>copyOf(events)).thenRun(state -> {
            for (PartitionEvent event : events) {
                switch (event) {
                    case PartitionEvent.Started started -> receivers.ready(state.attempt(started));
                    default -> {
                    }
                }
            }
            settle(state);
        });
    }

    /**
     * After a command: the timer at the next deadline; empty - the idle timer, counted from when it became empty.
     */
    private void settle(Partition state) {
        final long now = now();
        final long deadline = state.nextDeadline();
        if (deadline > now) {
            timers.startSingleTimer(TICK, new Tick(), Duration.ofMillis(deadline - now));
        } else {
            timers.cancel(TICK);
        }
        if (!state.isEmpty() || pending != null) {
            timers.cancel(IDLE);
        } else if (!timers.isTimerActive(IDLE)) {
            timers.startSingleTimer(IDLE, new Idle(), settings.idleTimeout());
        }
    }

    private static String keyOf(String entityId) {
        return entityId.startsWith(KEY_PREFIX)
            ? entityId.substring(KEY_PREFIX.length())
            : null;
    }

    private static long now() {
        return System.currentTimeMillis();
    }

    /**
     * A step waiting for the delivery of its final result.
     */
    private record Pending(
        Step step,
        ActorRef<StatusReply<Done>> replyTo
    ) {
    }
}
