package io.github.deripas.taskgrid.mailbox.memory;

import io.github.deripas.taskgrid.mailbox.Box;
import io.github.deripas.taskgrid.mailbox.MailboxSettings;
import io.github.deripas.taskgrid.mailbox.Reply;
import io.github.deripas.taskgrid.mailbox.Reservation;
import io.github.deripas.taskgrid.mailbox.SupersededException;
import io.github.deripas.taskgrid.task.RequestId;
import io.github.deripas.taskgrid.task.TaskResult;
import io.github.deripas.taskgrid.task.TaskStatus;
import org.junit.jupiter.api.Test;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Flow;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;

class MemoryMailboxTest {
    private static final Box BOX = new Box("pagenator", "0");
    private static final Duration TTL = Duration.ofMinutes(10);

    private final MutableClock clock = new MutableClock();
    private final MemoryMailbox mailbox = new MemoryMailbox(new MailboxSettings(2, TTL), clock);

    @Test
    void reservesUpToTheLimit() {
        assertEquals(Reservation.NEW, reserve("a"));
        assertEquals(Reservation.NEW, reserve("b"));
        assertEquals(Reservation.FULL, reserve("c"));
        assertEquals(Reservation.NEW, mailbox.reserve(new RequestId("pagenator", "1", "c")).toCompletableFuture().join());
    }

    @Test
    void reserveIsIdempotentByTaskId() {
        assertEquals(Reservation.NEW, reserve("a"));
        assertEquals(Reservation.PENDING, reserve("a"));

        put("a");
        assertEquals(Reservation.DONE, reserve("a"));
    }

    @Test
    void ackFreesThePlace() {
        reserve("a");
        reserve("b");
        put("a");
        mailbox.ack(BOX, List.of("a"));

        assertEquals(Reservation.NEW, reserve("c"));
    }

    @Test
    void releaseFreesAReservationButNotAResult() {
        reserve("a");
        reserve("b");
        put("b");
        mailbox.release(BOX.request("a"));
        mailbox.release(BOX.request("b"));

        assertEquals(Reservation.NEW, reserve("a"));
        assertEquals(Reservation.DONE, reserve("b"));
    }

    @Test
    void subscriberGetsResultsInTheOrderTheyWerePut() {
        put("a");
        final Recorder recorder = subscribe(Long.MAX_VALUE);
        put("b");

        assertEquals(List.of("a", "b"), recorder.ids());
    }

    @Test
    void subscriberGetsNoMoreThanItRequests() {
        put("a");
        put("b");
        final Recorder recorder = subscribe(1);
        assertEquals(List.of("a"), recorder.ids());

        recorder.subscription.request(1);
        assertEquals(List.of("a", "b"), recorder.ids());
    }

    @Test
    void resultNotAcknowledgedComesAgainWithTheNextSubscription() {
        put("a");
        put("b");
        final Recorder first = subscribe(Long.MAX_VALUE);
        mailbox.ack(BOX, List.of("a"));
        first.subscription.cancel();

        assertEquals(List.of("b"), subscribe(Long.MAX_VALUE).ids());
    }

    @Test
    void resultPutAgainIsNotSentAgainWithinTheSubscription() {
        final Recorder recorder = subscribe(Long.MAX_VALUE);
        put("a");
        put("a");

        assertEquals(List.of("a"), recorder.ids());
    }

    @Test
    void newerSubscriptionSupersedesTheOlderOne() {
        put("a");
        final Recorder older = subscribe(Long.MAX_VALUE);
        final Recorder newer = subscribe(Long.MAX_VALUE);
        put("b");

        assertInstanceOf(SupersededException.class, older.error);
        assertEquals(List.of("a"), older.ids());
        assertEquals(List.of("a", "b"), newer.ids());
    }

    @Test
    void cancelledSubscriptionGetsNothingMore() {
        final Recorder recorder = subscribe(Long.MAX_VALUE);
        recorder.subscription.cancel();
        put("a");

        assertEquals(List.of(), recorder.ids());
        assertNull(recorder.error);
    }

    @Test
    void nonPositiveRequestFailsTheSubscription() {
        final Recorder recorder = subscribe(0);

        assertInstanceOf(IllegalArgumentException.class, recorder.error);
    }

    @Test
    void resultWithoutReservationIsKeptAboveTheLimit() {
        reserve("a");
        reserve("b");
        put("c");

        assertEquals(List.of("c"), subscribe(Long.MAX_VALUE).ids());
    }

    @Test
    void tasksExpireAfterTtl() {
        reserve("a");
        put("b");
        clock.advance(TTL.minusMinutes(1));
        reserve("a");
        clock.advance(Duration.ofMinutes(2));

        assertEquals(Reservation.PENDING, reserve("a"));
        assertEquals(Reservation.NEW, reserve("b"));
        assertEquals(List.of(), subscribe(Long.MAX_VALUE).ids());
    }

    private Reservation reserve(String id) {
        return mailbox.reserve(BOX.request(id)).toCompletableFuture().join();
    }

    private void put(String id) {
        mailbox.put(BOX.request(id), result());
    }

    private Recorder subscribe(long request) {
        final Recorder recorder = new Recorder(request);
        mailbox.subscribe(BOX, recorder);
        return recorder;
    }

    /**
     * The same task may serve several requests: the reply is told by the id of the request.
     */
    private static TaskResult result() {
        return new TaskResult("work", TaskStatus.DONE, null, null, Map.of());
    }

    private static final class Recorder implements Flow.Subscriber<Reply> {
        private final long request;
        private final List<Reply> replies = new ArrayList<>();
        private Flow.Subscription subscription;
        private Throwable error;

        private Recorder(long request) {
            this.request = request;
        }

        @Override
        public void onSubscribe(Flow.Subscription subscription) {
            this.subscription = subscription;
            subscription.request(request);
        }

        @Override
        public void onNext(Reply item) {
            replies.add(item);
        }

        @Override
        public void onError(Throwable throwable) {
            error = throwable;
        }

        @Override
        public void onComplete() {
        }

        private List<String> ids() {
            return replies.stream().map(Reply::id).toList();
        }
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
