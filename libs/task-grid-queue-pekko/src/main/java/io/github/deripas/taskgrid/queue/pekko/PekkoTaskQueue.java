package io.github.deripas.taskgrid.queue.pekko;

import com.typesafe.config.Config;
import io.github.deripas.taskgrid.queue.DelayPolicy;
import io.github.deripas.taskgrid.queue.QueueSettings;
import io.github.deripas.taskgrid.queue.ReadyCallback;
import io.github.deripas.taskgrid.queue.ResultCallback;
import io.github.deripas.taskgrid.queue.TaskQueue;
import io.github.deripas.taskgrid.result.ResultStore;
import io.github.deripas.taskgrid.task.AttemptResult;
import io.github.deripas.taskgrid.task.Task;
import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;
import org.apache.pekko.Done;
import org.apache.pekko.actor.typed.ActorSystem;
import org.apache.pekko.cluster.sharding.typed.ClusterShardingSettings;
import org.apache.pekko.cluster.sharding.typed.javadsl.ClusterSharding;
import org.apache.pekko.cluster.sharding.typed.javadsl.Entity;
import org.apache.pekko.pattern.StatusReply;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;

/**
 * Persistent task queue of a Pekko cluster: each partition is a sharded event-sourced entity
 * ({@code PartitionEntity}) on some node of the cluster, recovered on another one if its node is gone.
 * <ul>
 *   <li>{@link #submit} completes when the task is in the journal.</li>
 *   <li>Ready attempts go to the {@link ReadyCallback}s of the node where their partition lives; connect them to
 *   a worker of that node ({@code Dispatcher}).</li>
 *   <li>{@link #complete} may be called on any node: the partition is found by the partition key in the meta of the
 *   result. A result lost on the way leaves the attempt to its deadline ({@link QueueSettings#attemptTimeout}).</li>
 *   <li>Final results go to the result store and the {@link ResultCallback}s of the node of the partition, again
 *   until all of them succeed (at least once), also after a restart.</li>
 * </ul>
 * The cluster, the journal and snapshot plugins and the serialization of the application are configured by the
 * application; settings of this library - {@code task-grid.queue.pekko} (reference.conf). Partitions are remembered
 * entities (event-sourced store): after a restart of the cluster those with work start by themselves.
 */
@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
public final class PekkoTaskQueue implements TaskQueue {
    private final ClusterSharding sharding;
    private final Receivers receivers;
    private final Duration askTimeout;

    /**
     * Starts the partitions on this node. Register the callbacks before the node joins the cluster or right after
     * start: partitions with work recover by themselves and hand out their attempts.
     */
    public static PekkoTaskQueue start(
        ActorSystem<?> system,
        QueueSettings settings,
        DelayPolicy delay,
        ResultStore results
    ) {
        final Config config = system.settings().config().getConfig("task-grid.queue.pekko");
        final Receivers receivers = new Receivers(results);
        final PartitionEntity.Settings entity = new PartitionEntity.Settings(settings, delay,
            config.getDuration("redeliver-after"), config.getInt("snapshot-every"), config.getDuration("idle-timeout"));
        final ClusterSharding sharding = ClusterSharding.get(system);
        sharding.init(Entity.of(PartitionEntity.TYPE_KEY, context ->
                PartitionEntity.create(context.getEntityId(), context.getShard(), entity, receivers))
            .withStopMessage(new PartitionEntity.Stop())
            .withSettings(ClusterShardingSettings.create(system)
                .withRememberEntities(true)
                .withRememberEntitiesStoreMode(ClusterShardingSettings.rememberEntitiesStoreModeEventSourced())));
        return new PekkoTaskQueue(sharding, receivers, config.getDuration("ask-timeout"));
    }

    /**
     * An id still in its partition with another payload fails with {@link IllegalArgumentException}.
     */
    @Override
    public CompletionStage<Void> submit(Task task) {
        return sharding.entityRefFor(PartitionEntity.TYPE_KEY, PartitionEntity.entityId(task.id(), task.meta()))
            .<Done>askWithStatus(replyTo -> new PartitionEntity.Submit(task, replyTo), askTimeout)
            .handle((done, error) -> {
                if (error == null) {
                    return CompletableFuture.<Void>completedFuture(null);
                }
                final Throwable cause = error instanceof CompletionException
                    ? error.getCause()
                    : error;
                return CompletableFuture.<Void>failedFuture(cause instanceof StatusReply.ErrorMessage rejected
                    ? new IllegalArgumentException(rejected.getMessage())
                    : cause);
            })
            .thenCompose(stage -> stage);
    }

    @Override
    public void complete(AttemptResult result) {
        sharding.entityRefFor(PartitionEntity.TYPE_KEY, PartitionEntity.entityId(result.taskId(), result.meta()))
            .tell(new PartitionEntity.Complete(result));
    }

    @Override
    public void onReady(ReadyCallback callback) {
        receivers.onReady(callback);
    }

    @Override
    public void onResult(ResultCallback callback) {
        receivers.onResult(callback);
    }
}
