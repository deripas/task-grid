package io.github.deripas.taskgrid.worker.remote.grpc;

import com.google.protobuf.ByteString;
import io.github.deripas.taskgrid.task.AttemptResult;
import io.github.deripas.taskgrid.task.AttemptStatus;
import io.github.deripas.taskgrid.task.Blob;
import io.github.deripas.taskgrid.worker.remote.Session;
import io.github.deripas.taskgrid.worker.remote.SessionChannel;
import io.github.deripas.taskgrid.worker.remote.SidecarHello;
import io.github.deripas.taskgrid.worker.remote.Sidecars;
import io.github.deripas.taskgrid.worker.remote.v1.BlobRef;
import io.github.deripas.taskgrid.worker.remote.v1.CompleteRequest;
import io.github.deripas.taskgrid.worker.remote.v1.CompleteResponse;
import io.github.deripas.taskgrid.worker.remote.v1.Status;
import io.grpc.Status.Code;
import io.grpc.stub.StreamObserver;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

/**
 * Translation of {@code Complete} to the domain: inline body, reference to blob storage, rejected requests; OK only once
 * the result is taken.
 */
class NodeServiceTest {
    private final List<AttemptResult> completed = new ArrayList<>();
    private CompletableFuture<Void> taken = CompletableFuture.completedFuture(null);
    private final NodeService service = new NodeService(new Sidecars() {
        @Override
        public Session connect(SidecarHello hello, SessionChannel channel) {
            throw new UnsupportedOperationException();
        }

        @Override
        public CompletionStage<Void> complete(String workerId, AttemptResult result) {
            completed.add(result);
            return taken;
        }
    });

    @Test
    void inlineBodyBecomesInlineBlob() {
        assertEquals(Code.OK, complete(done().setBody(ByteString.copyFromUtf8("answer")).build()));

        assertEquals(Blob.of("answer".getBytes(StandardCharsets.UTF_8)), completed.getFirst().body());
    }

    /**
     * The queue did not take the result (its journal is down): not OK, so the sidecar sends it again.
     */
    @Test
    void resultNotTakenIsUnavailable() {
        taken = CompletableFuture.failedFuture(new IllegalStateException("journal down"));

        assertEquals(Code.UNAVAILABLE, complete(done().setBody(ByteString.EMPTY).build()));
    }

    @Test
    void metaOfTheAttemptComesBack() {
        assertEquals(Code.OK, complete(done().putMeta("partition-key", "a.com").setBody(ByteString.EMPTY).build()));

        assertEquals(Map.of("partition-key", "a.com"), completed.getFirst().meta());
    }

    @Test
    void blobReferenceBecomesRef() {
        final BlobRef ref = BlobRef.newBuilder()
            .setLocation("s3://results/t1/abc")
            .setSize(100 << 20)
            .setSha256("ab12")
            .setEncoding("zstd")
            .build();

        assertEquals(Code.OK, complete(done().setBlob(ref).build()));

        assertEquals(new Blob.Ref("s3://results/t1/abc", 100 << 20, "ab12", "zstd"), completed.getFirst().body());
    }

    @Test
    void emptyEncodingMeansStoredAsIs() {
        final BlobRef ref = BlobRef.newBuilder().setLocation("s3://results/t1/abc").setSize(1).setSha256("ab12").build();

        complete(done().setBlob(ref).build());

        assertNull(((Blob.Ref) completed.getFirst().body()).encoding());
    }

    @Test
    void blobReferenceWithoutLocationIsRejected() {
        final BlobRef ref = BlobRef.newBuilder().setSize(1).setSha256("ab12").build();

        assertEquals(Code.INVALID_ARGUMENT, complete(done().setBlob(ref).build()));
        assertEquals(List.of(), completed);
    }

    @Test
    void failureHasNoBody() {
        assertEquals(Code.OK, complete(done().setStatus(Status.FAILED).setError("bad request").build()));

        assertEquals(AttemptStatus.FAILED, completed.getFirst().status());
        assertNull(completed.getFirst().body());
        assertEquals("bad request", completed.getFirst().error());
    }

    private static CompleteRequest.Builder done() {
        return CompleteRequest.newBuilder().setWorkerId("w1").setTaskId("t1").setAttempt(1).setStatus(Status.DONE);
    }

    /**
     * Calls Complete and returns the code of its answer.
     */
    private Code complete(CompleteRequest request) {
        final Code[] code = new Code[1];
        service.complete(request, new StreamObserver<>() {
            @Override
            public void onNext(CompleteResponse value) {
            }

            @Override
            public void onError(Throwable t) {
                code[0] = io.grpc.Status.fromThrowable(t).getCode();
            }

            @Override
            public void onCompleted() {
                code[0] = Code.OK;
            }
        });
        return code[0];
    }
}
