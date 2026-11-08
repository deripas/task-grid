package io.github.deripas.taskgrid.gateway.grpc;

import com.google.protobuf.ByteString;
import io.github.deripas.taskgrid.dispatch.Dispatcher;
import io.github.deripas.taskgrid.gateway.Gateway;
import io.github.deripas.taskgrid.gateway.InvalidPayloadException;
import io.github.deripas.taskgrid.gateway.TaskCodec;
import io.github.deripas.taskgrid.gateway.grpc.v1.AckReply;
import io.github.deripas.taskgrid.gateway.grpc.v1.AckRequest;
import io.github.deripas.taskgrid.gateway.grpc.v1.Result;
import io.github.deripas.taskgrid.gateway.grpc.v1.SubmitRequest;
import io.github.deripas.taskgrid.gateway.grpc.v1.SubscribeRequest;
import io.github.deripas.taskgrid.gateway.grpc.v1.TaskGatewayGrpc;
import io.github.deripas.taskgrid.mailbox.MailboxSettings;
import io.github.deripas.taskgrid.mailbox.MailboxSink;
import io.github.deripas.taskgrid.mailbox.memory.MemoryMailbox;
import io.github.deripas.taskgrid.queue.DelayPolicy;
import io.github.deripas.taskgrid.queue.QueueSettings;
import io.github.deripas.taskgrid.queue.TaskQueue;
import io.github.deripas.taskgrid.queue.memory.MemoryTaskQueue;
import io.github.deripas.taskgrid.task.Blob;
import io.github.deripas.taskgrid.task.Task;
import io.github.deripas.taskgrid.task.TaskResult;
import io.github.deripas.taskgrid.worker.Worker;
import io.github.deripas.taskgrid.worker.local.LocalWorker;
import io.grpc.Context;
import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.stub.StreamObserver;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * The gateway on an in-memory node with an in-process handler, over in-process gRPC. Protocol of the test: the payload
 * is a word, the reply - {@code done:<word>}; an empty payload is refused.
 */
class GatewayServiceTest {
    private static final int LIMIT = 2;
    private static final long WAIT_S = 5;

    private final ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor();
    private final AtomicInteger handled = new AtomicInteger();
    /**
     * The handler waits for it: tasks stay in the queue while the test submits.
     */
    private volatile CompletableFuture<Void> gate = CompletableFuture.completedFuture(null);
    private MemoryTaskQueue queue;
    private Server server;
    private ManagedChannel channel;
    private TaskGatewayGrpc.TaskGatewayBlockingStub blocking;
    private TaskGatewayGrpc.TaskGatewayStub async;

    @BeforeEach
    void start() throws IOException {
        queue = new MemoryTaskQueue(new QueueSettings(3, Duration.ofMinutes(1)), DelayPolicy.NONE);
        final MemoryMailbox mailbox = new MemoryMailbox(new MailboxSettings(LIMIT, Duration.ofMinutes(1)), Clock.systemUTC());
        queue.onResult(new MailboxSink(mailbox));
        Dispatcher.connect(queue, new LocalWorker(Map.of("echo", attempt -> {
            handled.incrementAndGet();
            gate.join();
            return attempt.task().payload();
        }), executor));
        final Gateway gateway = new Gateway(queue, mailbox, "node-1");
        final String name = InProcessServerBuilder.generateName();
        server = InProcessServerBuilder.forName(name).addService(new GatewayService(gateway, new WordCodec(), executor)).build().start();
        channel = InProcessChannelBuilder.forName(name).build();
        blocking = TaskGatewayGrpc.newBlockingStub(channel);
        async = TaskGatewayGrpc.newStub(channel);
    }

    @AfterEach
    void stop() {
        channel.shutdownNow();
        server.shutdownNow();
        executor.shutdownNow();
        queue.close();
    }

    @Test
    void resultComesToTheSubscriptionAndLeavesTheBoxOnAck() throws InterruptedException {
        submit("pagenator", "0", "r1", "a");
        final Subscription subscription = subscribe("pagenator", "0");

        final Result result = subscription.take();
        assertEquals("r1", result.getId());
        assertEquals("done:a", result.getPayload().toStringUtf8());

        ack("pagenator", "0", "r1");
        subscription.cancel();
        assertNull(subscribe("pagenator", "0").poll(), "acknowledged");
    }

    @Test
    void resultNotAcknowledgedComesAgainAndANewerSubscriptionEndsTheOlderOne() throws InterruptedException {
        submit("pagenator", "0", "r1", "a");
        final Subscription older = subscribe("pagenator", "0");
        assertEquals("r1", older.take().getId());

        final Subscription newer = subscribe("pagenator", "0");
        assertEquals("r1", newer.take().getId());
        assertEquals(Status.Code.ABORTED, older.error().getStatus().getCode());
    }

    /**
     * The same request again is one request; the same payload of another client is one task: done once, both get it.
     */
    @Test
    void samePayloadIsDoneOnceForEveryRequest() throws InterruptedException {
        gate = new CompletableFuture<>();
        submit("pagenator", "0", "r1", "a");
        submit("pagenator", "0", "r1", "a");
        submit("indexer", "0", "x1", "a");
        gate.complete(null);

        final Subscription pagenator = subscribe("pagenator", "0");
        assertEquals("r1", pagenator.take().getId());
        assertNull(pagenator.poll(), "one result of the request");
        assertEquals("x1", subscribe("indexer", "0").take().getId());
        assertEquals(1, handled.get());

        submit("pagenator", "0", "r1", "a");
        assertEquals(1, handled.get(), "its result waits in the box: not queued again");
    }

    @Test
    void fullBoxRefusesUntilResultsAreAcknowledged() throws InterruptedException {
        submit("pagenator", "0", "r1", "a");
        submit("pagenator", "0", "r2", "b");

        final StatusRuntimeException full = assertThrows(StatusRuntimeException.class, () -> submit("pagenator", "0", "r3", "c"));
        assertEquals(Status.Code.RESOURCE_EXHAUSTED, full.getStatus().getCode());
        submit("pagenator", "1", "r3", "c");

        subscribe("pagenator", "0").take();
        ack("pagenator", "0", "r1");
        submit("pagenator", "0", "r3", "c");
    }

    @Test
    void badRequestIsInvalidArgument() {
        final StatusRuntimeException empty = assertThrows(StatusRuntimeException.class, () -> submit("pagenator", "0", "r1", ""));
        assertEquals(Status.Code.INVALID_ARGUMENT, empty.getStatus().getCode());
        final StatusRuntimeException noClient = assertThrows(StatusRuntimeException.class, () -> submit("", "0", "r1", "a"));
        assertEquals(Status.Code.INVALID_ARGUMENT, noClient.getStatus().getCode());
    }

    private void submit(
        String client,
        String consumer,
        String id,
        String word
    ) {
        blocking.submit(SubmitRequest.newBuilder()
            .setClient(client).setConsumer(consumer).setId(id)
            .setPayload(ByteString.copyFromUtf8(word))
            .build());
    }

    private Subscription subscribe(String client, String consumer) {
        final Subscription subscription = new Subscription();
        final SubscribeRequest request = SubscribeRequest.newBuilder().setClient(client).setConsumer(consumer).build();
        subscription.context.run(() -> async.subscribe(request, subscription));
        return subscription;
    }

    private void ack(
        String client,
        String consumer,
        String... ids
    ) {
        final CompletableFuture<AckReply> done = new CompletableFuture<>();
        final StreamObserver<AckRequest> acks = async.ack(new StreamObserver<>() {
            @Override
            public void onNext(AckReply reply) {
                done.complete(reply);
            }

            @Override
            public void onError(Throwable error) {
                done.completeExceptionally(error);
            }

            @Override
            public void onCompleted() {
            }
        });
        acks.onNext(AckRequest.newBuilder().setClient(client).setConsumer(consumer).addAllIds(List.of(ids)).build());
        acks.onCompleted();
        done.orTimeout(WAIT_S, TimeUnit.SECONDS).join();
    }

    /**
     * The client side of a subscription.
     */
    private final class Subscription implements StreamObserver<Result> {
        private final BlockingQueue<Result> results = new LinkedBlockingQueue<>();
        private final CompletableFuture<StatusRuntimeException> ended = new CompletableFuture<>();
        private final Context.CancellableContext context = Context.current().withCancellation();

        @Override
        public void onNext(Result result) {
            results.add(result);
        }

        @Override
        public void onError(Throwable error) {
            ended.complete((StatusRuntimeException) error);
        }

        @Override
        public void onCompleted() {
            ended.complete(null);
        }

        Result take() throws InterruptedException {
            final Result result = results.poll(WAIT_S, TimeUnit.SECONDS);
            assertNotNull(result, "no result within " + WAIT_S + " s");
            return result;
        }

        Result poll() throws InterruptedException {
            return results.poll(300, TimeUnit.MILLISECONDS);
        }

        StatusRuntimeException error() {
            return ended.orTimeout(WAIT_S, TimeUnit.SECONDS).join();
        }

        void cancel() {
            context.cancel(null);
        }
    }

    /**
     * A word is a task of kind echo; the reply is {@code done:} and the word.
     */
    private static final class WordCodec implements TaskCodec {
        @Override
        public Task task(byte[] payload) throws InvalidPayloadException {
            if (payload.length == 0) {
                throw new InvalidPayloadException("empty word");
            }
            return Task.of(Blob.of(payload), Map.of(Worker.KIND, "echo", TaskQueue.PARTITION_KEY, "words"));
        }

        @Override
        public byte[] reply(String id, TaskResult result) {
            final String word = new String(((Blob.Inline) result.body()).bytes(), StandardCharsets.UTF_8);
            return ("done:" + word).getBytes(StandardCharsets.UTF_8);
        }

        @Override
        public byte[] refusal(
            String id,
            Refusal refusal,
            String message
        ) {
            return (refusal + ":" + message).getBytes(StandardCharsets.UTF_8);
        }
    }
}
