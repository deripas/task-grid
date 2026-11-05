package io.github.deripas.taskgrid.worker.remote.grpc;

import com.google.protobuf.ByteString;
import io.github.deripas.taskgrid.worker.remote.v1.Attempt;
import io.github.deripas.taskgrid.worker.remote.v1.CompleteRequest;
import io.github.deripas.taskgrid.worker.remote.v1.Credit;
import io.github.deripas.taskgrid.worker.remote.v1.Hello;
import io.github.deripas.taskgrid.worker.remote.v1.NodeGrpc;
import io.github.deripas.taskgrid.worker.remote.v1.NodeMessage;
import io.github.deripas.taskgrid.worker.remote.v1.SidecarMessage;
import io.github.deripas.taskgrid.worker.remote.v1.Status;
import io.grpc.Channel;
import io.grpc.stub.StreamObserver;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Minimal sidecar for tests, in place of handler-sidecar: one session, credit by the demand of the node up to capacity,
 * surplus credit taken back when the node returns it. A handler exception is a RETRY.
 */
final class TestSidecar implements AutoCloseable {
    /**
     * The handler behind the sidecar.
     */
    @FunctionalInterface
    interface Handler {
        byte[] handle(Attempt attempt) throws Exception;
    }

    private final String workerId;
    private final Handler handler;
    private final NodeGrpc.NodeBlockingStub results;
    private final ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor();
    private final List<Attempt> received = new CopyOnWriteArrayList<>();
    private final CompletableFuture<io.grpc.Status> ended = new CompletableFuture<>();
    private final StreamObserver<SidecarMessage> requests;
    private final int capacity;
    // session numbers: credit granted up to limit, attempts finished, demand of the node
    private long limit;
    private long version;
    private long finished;
    private long target;
    private boolean draining;
    private boolean closed;

    TestSidecar(
        Channel channel,
        String workerId,
        String kind,
        int capacity,
        Handler handler
    ) {
        this.workerId = workerId;
        this.handler = handler;
        this.capacity = capacity;
        this.results = NodeGrpc.newBlockingStub(channel);
        this.requests = NodeGrpc.newStub(channel).connect(new Responses());
        send(SidecarMessage.newBuilder()
            .setHello(Hello.newBuilder()
                .setWorkerId(workerId)
                .setInstanceId(UUID.randomUUID().toString())
                .setKind(kind)
                .setCapacity(capacity))
            .build());
    }

    /**
     * Attempts received from the node, in order.
     */
    List<Attempt> received() {
        return received;
    }

    String workerId() {
        return workerId;
    }

    /**
     * Completes with the status the node ended the session with.
     */
    CompletableFuture<io.grpc.Status> ended() {
        return ended;
    }

    /**
     * Sends a raw message on the session stream.
     */
    synchronized void send(SidecarMessage message) {
        if (!closed) {
            requests.onNext(message);
        }
    }

    /**
     * Breaks the session as a crashed process would: no results for the attempts in progress.
     */
    void crash() {
        executor.shutdownNow();
        synchronized (this) {
            closed = true;
            requests.onError(new IllegalStateException("crash"));
        }
    }

    /**
     * Graceful shutdown: no more credit, half-close of the stream; attempts in progress still run and report results.
     */
    synchronized void drain() {
        draining = true;
        if (!closed) {
            closed = true;
            requests.onCompleted();
        }
    }

    @Override
    public void close() {
        drain();
        executor.close();
    }

    private synchronized void demanded(long demandTarget, long demandVersion) {
        target = demandTarget;
        if (demandVersion == version && demandTarget < limit) {
            limit = demandTarget; // the node returned surplus credit
        }
        grant();
    }

    private synchronized void finished() {
        finished++;
        grant();
    }

    /**
     * Credit up to the demand, as long as attempts in progress plus unused credit stay within capacity.
     */
    private synchronized void grant() {
        if (draining) {
            return;
        }
        final long wanted = Math.min(target, finished + capacity);
        if (wanted > limit) {
            limit = wanted;
            version++;
            send(SidecarMessage.newBuilder().setCredit(Credit.newBuilder().setLimit(limit).setVersion(version)).build());
        }
    }

    private void execute(Attempt attempt) {
        final CompleteRequest.Builder result = CompleteRequest.newBuilder()
            .setWorkerId(workerId)
            .setTaskId(attempt.getTaskId())
            .setAttempt(attempt.getNumber())
            .putAllMeta(attempt.getMetaMap());
        try {
            result.setStatus(Status.DONE).setBody(ByteString.copyFrom(handler.handle(attempt)));
        } catch (InterruptedException e) {
            return;
        } catch (Exception e) {
            result.setStatus(Status.RETRY).setError(String.valueOf(e));
        }
        results.complete(result.build());
        finished();
    }

    private final class Responses implements StreamObserver<NodeMessage> {
        @Override
        public void onNext(NodeMessage message) {
            if (message.hasAttempt()) {
                received.add(message.getAttempt());
                executor.execute(() -> execute(message.getAttempt()));
            } else if (message.hasDemand()) {
                demanded(message.getDemand().getTarget(), message.getDemand().getVersion());
            }
        }

        @Override
        public void onError(Throwable error) {
            ended.complete(io.grpc.Status.fromThrowable(error));
        }

        @Override
        public void onCompleted() {
            ended.complete(io.grpc.Status.OK);
        }
    }
}
