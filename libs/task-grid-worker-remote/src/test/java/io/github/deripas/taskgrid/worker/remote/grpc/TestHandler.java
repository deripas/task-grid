package io.github.deripas.taskgrid.worker.remote.grpc;

import io.grpc.Context;
import io.grpc.Contexts;
import io.grpc.Grpc;
import io.grpc.InsecureServerCredentials;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;
import io.grpc.Server;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import io.grpc.ServerInterceptors;
import io.grpc.ServerServiceDefinition;
import io.grpc.Status;
import io.grpc.stub.ServerCalls;
import io.grpc.stub.StreamObserver;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Handler for tests behind the real handler-sidecar: a gRPC server with one unary method of raw bytes, like any
 * handler with its own types. The request is a text command:
 * <ul>
 *   <li>{@code fail} - answers INVALID_ARGUMENT;</li>
 *   <li>{@code sleep:<ms>} - answers after a pause;</li>
 *   <li>anything else - answers at once.</li>
 * </ul>
 * The answer is the request, then the received {@code x-*} metadata as {@code key=value} lines, sorted.
 */
final class TestHandler implements AutoCloseable {
    static final String METHOD = "test.v1.Handler/Call";

    private static final Context.Key<Metadata> HEADERS = Context.key("headers");
    private static final MethodDescriptor.Marshaller<byte[]> BYTES = new MethodDescriptor.Marshaller<>() {
        @Override
        public InputStream stream(byte[] value) {
            return new ByteArrayInputStream(value);
        }

        @Override
        public byte[] parse(InputStream stream) {
            try {
                return stream.readAllBytes();
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
    };

    private final AtomicInteger calls = new AtomicInteger();
    private final AtomicInteger running = new AtomicInteger();
    private final AtomicInteger peak = new AtomicInteger();
    private final Server server;

    TestHandler() throws IOException {
        final MethodDescriptor<byte[], byte[]> method = MethodDescriptor.<byte[], byte[]>newBuilder()
            .setType(MethodDescriptor.MethodType.UNARY)
            .setFullMethodName(METHOD)
            .setRequestMarshaller(BYTES)
            .setResponseMarshaller(BYTES)
            .build();
        final ServerServiceDefinition service = ServerServiceDefinition.builder("test.v1.Handler")
            .addMethod(method, ServerCalls.asyncUnaryCall(this::call))
            .build();
        final ServerInterceptor headers = new ServerInterceptor() {
            @Override
            public <Q, R> ServerCall.Listener<Q> interceptCall(
                ServerCall<Q, R> call,
                Metadata metadata,
                ServerCallHandler<Q, R> next
            ) {
                return Contexts.interceptCall(Context.current().withValue(HEADERS, metadata), call, metadata, next);
            }
        };
        this.server = Grpc.newServerBuilderForPort(0, InsecureServerCredentials.create())
            .addService(ServerInterceptors.intercept(service, headers))
            .build()
            .start();
    }

    int port() {
        return server.getPort();
    }

    int calls() {
        return calls.get();
    }

    /**
     * The largest number of concurrent calls.
     */
    int peak() {
        return peak.get();
    }

    @Override
    public void close() throws InterruptedException {
        server.shutdownNow().awaitTermination(5, TimeUnit.SECONDS);
    }

    private void call(byte[] request, StreamObserver<byte[]> response) {
        calls.incrementAndGet();
        peak.accumulateAndGet(running.incrementAndGet(), Math::max);
        try {
            final String command = new String(request, StandardCharsets.UTF_8);
            if (command.equals("fail")) {
                response.onError(Status.INVALID_ARGUMENT.withDescription("test handler: bad request").asRuntimeException());
                return;
            }
            if (command.startsWith("sleep:")) {
                Thread.sleep(Long.parseLong(command.substring("sleep:".length())));
            }
            final StringBuilder answer = new StringBuilder(command);
            headers().forEach((k, v) -> answer.append('\n').append(k).append('=').append(v));
            response.onNext(answer.toString().getBytes(StandardCharsets.UTF_8));
            response.onCompleted();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            response.onError(Status.CANCELLED.asRuntimeException());
        } finally {
            running.decrementAndGet();
        }
    }

    private static Map<String, String> headers() {
        final Metadata metadata = HEADERS.get();
        final Map<String, String> out = new TreeMap<>();
        for (String key : metadata.keys()) {
            if (key.startsWith("x-") && !key.endsWith(Metadata.BINARY_HEADER_SUFFIX)) {
                out.put(key, metadata.get(Metadata.Key.of(key, Metadata.ASCII_STRING_MARSHALLER)));
            }
        }
        return out;
    }
}
