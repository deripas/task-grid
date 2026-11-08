package io.github.deripas.taskgrid.gateway;

import io.github.deripas.taskgrid.mailbox.Box;
import io.github.deripas.taskgrid.mailbox.Mailbox;
import io.github.deripas.taskgrid.mailbox.MailboxSink;
import io.github.deripas.taskgrid.mailbox.Reply;
import io.github.deripas.taskgrid.mailbox.Reservation;
import io.github.deripas.taskgrid.queue.TaskQueue;
import io.github.deripas.taskgrid.task.RequestId;
import io.github.deripas.taskgrid.task.Task;
import io.github.deripas.taskgrid.task.TaskResult;
import lombok.extern.slf4j.Slf4j;

import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Flow;
import java.util.concurrent.TimeUnit;

/**
 * Clients of the queue whose results wait in the mailbox, whatever the transport. A request takes a place in the box of
 * its consumer, then goes to the queue marked for the mailbox ({@link MailboxSink}, registered on every node with the
 * queue). Then either the consumer subscribes to its box and acknowledges what it has processed ({@link #submit}), or
 * the request is waited for on this node ({@link #call}): the box of the client and this node, whose replies this
 * node hands to the waiting requests.
 */
@Slf4j
public final class Gateway {
    private final TaskQueue queue;
    private final Mailbox mailbox;
    /**
     * The consumer of the boxes of this node.
     */
    private final String node;
    /**
     * Clients waiting on this node: subscribed to their boxes.
     */
    private final Set<String> subscribed = ConcurrentHashMap.newKeySet();
    private final Map<RequestId, CompletableFuture<TaskResult>> waiters = new ConcurrentHashMap<>();

    /**
     * @param node this node among the nodes: the consumer of the boxes of the requests waited for here
     */
    public Gateway(
        TaskQueue queue,
        Mailbox mailbox,
        String node
    ) {
        this.queue = queue;
        this.mailbox = mailbox;
        this.node = node;
    }

    /**
     * Idempotent by the request id: a request in the queue is submitted again (the queue keeps it once), a request
     * whose result waits in the box is not.
     */
    public CompletionStage<Submission> submit(RequestId request, Task task) {
        return mailbox.reserve(request).thenCompose(reservation -> switch (reservation) {
            case FULL -> CompletableFuture.completedFuture(new Submission.Busy());
            case DONE -> CompletableFuture.completedFuture(new Submission.Accepted());
            case NEW, PENDING -> queue.submit(task, MailboxSink.request(request))
                .handle((ignored, error) -> error == null
                    ? CompletableFuture.<Submission>completedFuture(new Submission.Accepted())
                    : failed(request, reservation, error))
                .thenCompose(stage -> stage);
        });
    }

    /**
     * Submits the request of a client waiting on this node and waits for its result. Calls with the same id wait for
     * the same request. The place in the box is freed when the result is taken, also after the caller stopped waiting.
     *
     * @param client the box of the client on this node
     * @param id     of the request among those of the client on this node
     */
    public CompletionStage<Call> call(
        String client,
        String id,
        Task task,
        Duration timeout
    ) {
        final RequestId request = new RequestId(client, node, id);
        final CompletableFuture<TaskResult> waiter = waiters.computeIfAbsent(request, r -> {
            final CompletableFuture<TaskResult> created = new CompletableFuture<>();
            created.whenComplete((result, error) -> waiters.remove(r, created));
            return created;
        });
        subscribe(client);
        return submit(request, task).thenCompose(submission -> switch (submission) {
            case Submission.Busy ignored -> CompletableFuture.completedFuture(new Call.Busy());
            case Submission.Rejected rejected -> CompletableFuture.completedFuture(new Call.Rejected(rejected.reason()));
            case Submission.Accepted ignored -> waiter.<Call>thenApply(Call.Done::new)
                .completeOnTimeout(new Call.TimedOut(), timeout.toMillis(), TimeUnit.MILLISECONDS);
        });
    }

    /**
     * Results of the box not acknowledged yet, then the new ones; see {@link Mailbox#subscribe}.
     */
    public void subscribe(Box box, Flow.Subscriber<? super Reply> subscriber) {
        mailbox.subscribe(box, subscriber);
    }

    public CompletionStage<Void> ack(Box box, Collection<String> ids) {
        return mailbox.ack(box, ids);
    }

    private void subscribe(String client) {
        if (subscribed.add(client)) {
            final Box box = new Box(client, node);
            mailbox.subscribe(box, new Waiting(box));
        }
    }

    /**
     * The queue did not take the request: a new one frees its place (an earlier submit of a pending one may have
     * succeeded); a conflict of ids is a refusal, anything else - a failure the client retries.
     */
    private CompletionStage<Submission> failed(
        RequestId request,
        Reservation reservation,
        Throwable error
    ) {
        final Throwable cause = error instanceof CompletionException && error.getCause() != null
            ? error.getCause()
            : error;
        if (reservation == Reservation.NEW) {
            mailbox.release(request);
        }
        if (cause instanceof IllegalArgumentException) {
            return CompletableFuture.completedFuture(new Submission.Rejected(cause.getMessage()));
        }
        log.warn("Request not queued: request={} error={}", request, cause.toString());
        return CompletableFuture.failedFuture(cause);
    }

    /**
     * Replies of a box of this node: each goes to its waiting request and is acknowledged at once; a reply nobody
     * waits for any more (timed out) is dropped. The subscription ended - the next call of the client subscribes again.
     */
    private final class Waiting implements Flow.Subscriber<Reply> {
        private final Box box;

        private Waiting(Box box) {
            this.box = box;
        }

        @Override
        public void onSubscribe(Flow.Subscription subscription) {
            subscription.request(Long.MAX_VALUE);
        }

        @Override
        public void onNext(Reply reply) {
            final CompletableFuture<TaskResult> waiter = waiters.get(box.request(reply.id()));
            if (waiter != null) {
                waiter.complete(reply.result());
            }
            mailbox.ack(box, List.of(reply.id()));
        }

        @Override
        public void onError(Throwable error) {
            log.warn("Replies of {} ended: {}", box, error.toString());
            subscribed.remove(box.client());
        }

        @Override
        public void onComplete() {
            subscribed.remove(box.client());
        }
    }
}
