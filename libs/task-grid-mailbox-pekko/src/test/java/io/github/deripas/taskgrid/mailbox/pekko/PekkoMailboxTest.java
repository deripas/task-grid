package io.github.deripas.taskgrid.mailbox.pekko;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import io.github.deripas.taskgrid.mailbox.Box;
import io.github.deripas.taskgrid.mailbox.MailboxSettings;
import io.github.deripas.taskgrid.mailbox.Reply;
import io.github.deripas.taskgrid.mailbox.Reservation;
import io.github.deripas.taskgrid.mailbox.SupersededException;
import io.github.deripas.taskgrid.task.Blob;
import io.github.deripas.taskgrid.task.RequestId;
import io.github.deripas.taskgrid.task.TaskResult;
import io.github.deripas.taskgrid.task.TaskStatus;
import org.apache.pekko.actor.testkit.typed.javadsl.ActorTestKit;
import org.apache.pekko.cluster.sharding.typed.javadsl.ClusterSharding;
import org.apache.pekko.cluster.typed.Cluster;
import org.apache.pekko.cluster.typed.Join;
import org.apache.pekko.persistence.testkit.PersistenceTestKitPlugin;
import org.apache.pekko.persistence.testkit.PersistenceTestKitSnapshotPlugin;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Flow;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

/**
 * The mailbox on a one-node cluster with the journal and snapshots in memory. Every message, event and snapshot goes
 * through its serializer, as between nodes and into a real journal.
 */
class PekkoMailboxTest {
    private static final Box BOX = new Box("pagenator", "0");
    private static final Duration WAIT = Duration.ofSeconds(10);

    private ActorTestKit testKit;
    private PekkoMailbox mailbox;

    @AfterEach
    void stop() {
        testKit.shutdownTestKit();
    }

    @Test
    void reservesUpToTheLimit() {
        start(new MailboxSettings(2, Duration.ofMinutes(1)));
        assertEquals(Reservation.NEW, reserve("a"));
        assertEquals(Reservation.PENDING, reserve("a"));
        assertEquals(Reservation.NEW, reserve("b"));
        assertEquals(Reservation.FULL, reserve("c"));

        mailbox.release(BOX.request("b")).toCompletableFuture().join();
        assertEquals(Reservation.NEW, reserve("c"));
        put("a");
        assertEquals(Reservation.DONE, reserve("a"));
    }

    @Test
    void subscriberGetsResultsInOrderWithinItsDemandAndAckRemovesThem() throws InterruptedException {
        start(new MailboxSettings(10, Duration.ofMinutes(1)));
        put("a");
        put("b");
        final Recorder recorder = subscribe(1);
        assertEquals("a", recorder.take().id());
        assertNull(recorder.poll(), "within the demand");

        recorder.subscription.join().request(1);
        assertEquals("b", recorder.take().id());
        put("c");
        assertNull(recorder.poll());

        mailbox.ack(BOX, List.of("a", "b")).toCompletableFuture().join();
        recorder.subscription.join().cancel();
        final Recorder next = subscribe(Long.MAX_VALUE);
        assertEquals("c", next.take().id());
        assertNull(next.poll(), "acknowledged results do not come again");
    }

    @Test
    void newerSubscriptionSupersedesTheOlderOne() throws InterruptedException {
        start(new MailboxSettings(10, Duration.ofMinutes(1)));
        put("a");
        final Recorder older = subscribe(Long.MAX_VALUE);
        assertEquals("a", older.take().id());

        final Recorder newer = subscribe(Long.MAX_VALUE);
        assertEquals("a", newer.take().id());
        assertInstanceOf(SupersededException.class, older.error.orTimeout(WAIT.toSeconds(), TimeUnit.SECONDS).join());
    }

    /**
     * The box stops (its node is gone, a rebalance): its requests are in the journal, the subscriber follows it and
     * gets again what is not acknowledged.
     */
    @Test
    void subscriptionFollowsTheBoxOverARestart() throws InterruptedException {
        start(new MailboxSettings(10, Duration.ofMinutes(1)));
        put("a");
        reserve("b");
        final Recorder recorder = subscribe(Long.MAX_VALUE);
        assertEquals("a", recorder.take().id());

        ClusterSharding.get(testKit.system()).entityRefFor(BoxEntity.TYPE_KEY, BOX.key()).tell(new BoxEntity.Stop());

        assertEquals("a", recorder.take().id(), "sent again after the restart");
        assertEquals(Reservation.PENDING, reserve("b"));
        put("b");
        assertEquals("b", recorder.take().id());
    }

    @Test
    void requestsExpireAfterTtl() throws InterruptedException {
        start(new MailboxSettings(1, Duration.ofMillis(500)));
        put("a");
        assertEquals(Reservation.FULL, reserve("b"));

        Thread.sleep(1_000);
        assertEquals(Reservation.NEW, reserve("b"));
        assertNull(subscribe(Long.MAX_VALUE).poll(), "the expired result is gone");
    }

    private void start(MailboxSettings settings) {
        testKit = ActorTestKit.create("mailbox", config());
        mailbox = PekkoMailbox.start(testKit.system(), settings);
        final Cluster cluster = Cluster.get(testKit.system());
        cluster.manager().tell(Join.create(cluster.selfMember().address()));
    }

    private Reservation reserve(String id) {
        return mailbox.reserve(BOX.request(id)).toCompletableFuture().join();
    }

    private void put(String id) {
        mailbox.put(new RequestId(BOX.client(), BOX.consumer(), id), result()).toCompletableFuture().join();
    }

    private Recorder subscribe(long request) {
        final Recorder recorder = new Recorder(request);
        mailbox.subscribe(BOX, recorder);
        return recorder;
    }

    private static TaskResult result() {
        return new TaskResult("work", TaskStatus.DONE, Blob.of(new byte[]{1}), null, Map.of("kind", "fetch"));
    }

    private static Config config() {
        return ConfigFactory.parseString("""
                pekko.actor.provider = cluster
                pekko.remote.artery.canonical.hostname = 127.0.0.1
                pekko.remote.artery.canonical.port = 0
                pekko.loggers = ["org.apache.pekko.event.slf4j.Slf4jLogger"]
                pekko.logging-filter = "org.apache.pekko.event.slf4j.Slf4jLoggingFilter"
                # every message is serialized, also the local ones: what goes between nodes must
                pekko.actor.serialize-messages = on
                pekko.cluster.jmx.multi-mbeans-in-same-jvm = on
                pekko.log-dead-letters = off
                pekko.cluster.sharding.entity-restart-backoff = 100ms
                task-grid.mailbox.pekko.heartbeat = 200ms
                task-grid.mailbox.pekko.snapshot-every = 3
                """)
            .withFallback(PersistenceTestKitPlugin.getInstance().config())
            .withFallback(PersistenceTestKitSnapshotPlugin.config())
            .withFallback(ConfigFactory.load());
    }

    /**
     * Records what the subscription delivers; called on a thread of the actor system.
     */
    private static final class Recorder implements Flow.Subscriber<Reply> {
        private final long request;
        private final BlockingQueue<Reply> replies = new LinkedBlockingQueue<>();
        private final CompletableFuture<Flow.Subscription> subscription = new CompletableFuture<>();
        private final CompletableFuture<Throwable> error = new CompletableFuture<>();

        private Recorder(long request) {
            this.request = request;
        }

        @Override
        public void onSubscribe(Flow.Subscription subscription) {
            this.subscription.complete(subscription);
            subscription.request(request);
        }

        @Override
        public void onNext(Reply item) {
            replies.add(item);
        }

        @Override
        public void onError(Throwable throwable) {
            error.complete(throwable);
        }

        @Override
        public void onComplete() {
        }

        Reply take() throws InterruptedException {
            final Reply reply = replies.poll(WAIT.toSeconds(), TimeUnit.SECONDS);
            assertNotNull(reply, "no reply within " + WAIT);
            return reply;
        }

        Reply poll() throws InterruptedException {
            return replies.poll(300, TimeUnit.MILLISECONDS);
        }
    }
}
