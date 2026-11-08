package io.github.deripas.taskgrid.mailbox.pekko;

import com.typesafe.config.Config;
import io.github.deripas.taskgrid.mailbox.Box;
import io.github.deripas.taskgrid.mailbox.Mailbox;
import io.github.deripas.taskgrid.mailbox.MailboxSettings;
import io.github.deripas.taskgrid.mailbox.Reply;
import io.github.deripas.taskgrid.mailbox.Reservation;
import io.github.deripas.taskgrid.task.RequestId;
import io.github.deripas.taskgrid.task.TaskResult;
import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;
import org.apache.pekko.Done;
import org.apache.pekko.actor.typed.ActorSystem;
import org.apache.pekko.actor.typed.Props;
import org.apache.pekko.cluster.sharding.typed.ClusterShardingSettings;
import org.apache.pekko.cluster.sharding.typed.javadsl.ClusterSharding;
import org.apache.pekko.cluster.sharding.typed.javadsl.Entity;
import org.apache.pekko.cluster.sharding.typed.javadsl.EntityRef;

import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Flow;

/**
 * Mailboxes of a Pekko cluster: each box is a sharded event-sourced entity ({@code BoxEntity}) on some node,
 * recovered on another one if its node is gone; any node reaches any box. A subscription is an actor on the node of
 * the subscriber; it follows its box when the box moves.
 * <p>
 * The cluster, the journal and snapshot plugins are configured by the application; settings of this library -
 * {@code task-grid.mailbox.pekko} (reference.conf). Boxes with requests are remembered entities: they start by
 * themselves after a restart of the cluster, and their requests expire.
 */
@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
public final class PekkoMailbox implements Mailbox {
    private final ActorSystem<?> system;
    private final ClusterSharding sharding;
    private final Duration askTimeout;
    private final Duration heartbeat;

    /**
     * Starts the boxes on this node.
     */
    public static PekkoMailbox start(ActorSystem<?> system, MailboxSettings settings) {
        final Config config = system.settings().config().getConfig("task-grid.mailbox.pekko");
        final BoxEntity.Settings entity = new BoxEntity.Settings(settings, config.getInt("snapshot-every"),
            config.getDuration("idle-timeout"));
        final ClusterSharding sharding = ClusterSharding.get(system);
        sharding.init(Entity.of(BoxEntity.TYPE_KEY, context -> BoxEntity.create(context.getEntityId(), context.getShard(), entity))
            .withStopMessage(new BoxEntity.Stop())
            .withSettings(ClusterShardingSettings.create(system)
                .withRememberEntities(true)
                .withRememberEntitiesStoreMode(ClusterShardingSettings.rememberEntitiesStoreModeEventSourced())));
        return new PekkoMailbox(system, sharding, config.getDuration("ask-timeout"), config.getDuration("heartbeat"));
    }

    @Override
    public CompletionStage<Reservation> reserve(RequestId request) {
        return entity(Box.of(request)).askWithStatus(replyTo -> new BoxEntity.Reserve(request.id(), replyTo), askTimeout);
    }

    @Override
    public CompletionStage<Void> release(RequestId request) {
        return entity(Box.of(request))
            .<Done>askWithStatus(replyTo -> new BoxEntity.Release(request.id(), replyTo), askTimeout)
            .thenApply(done -> null);
    }

    @Override
    public CompletionStage<Void> put(RequestId request, TaskResult result) {
        return entity(Box.of(request))
            .<Done>askWithStatus(replyTo -> new BoxEntity.Put(request.id(), result, replyTo), askTimeout)
            .thenApply(done -> null);
    }

    /**
     * The subscriber is called on a thread of the actor system: it must not block.
     */
    @Override
    public void subscribe(Box box, Flow.Subscriber<? super Reply> subscriber) {
        system.systemActorOf(BoxSubscriber.create(entity(box), box, subscriber, heartbeat),
            "task-grid-box-subscriber-" + UUID.randomUUID(), Props.empty());
    }

    @Override
    public CompletionStage<Void> ack(Box box, Collection<String> ids) {
        final List<String> copy = List.copyOf(ids);
        return entity(box)
            .<Done>askWithStatus(replyTo -> new BoxEntity.Ack(copy, replyTo), askTimeout)
            .thenApply(done -> null);
    }

    private EntityRef<BoxEntity.Command> entity(Box box) {
        return sharding.entityRefFor(BoxEntity.TYPE_KEY, box.key());
    }
}
