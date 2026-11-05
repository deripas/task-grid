package io.github.deripas.crawler.urlmanager.grpc;

import io.github.deripas.taskgrid.worker.remote.Load;
import io.github.deripas.taskgrid.worker.remote.RemoteSettings;
import io.github.deripas.taskgrid.worker.remote.RemoteWorker;
import io.github.deripas.taskgrid.worker.remote.grpc.NodeService;
import io.github.deripas.taskgrid.worker.remote.v1.Hello;
import io.github.deripas.taskgrid.worker.remote.v1.NodeGrpc;
import io.github.deripas.taskgrid.worker.remote.v1.NodeMessage;
import io.github.deripas.taskgrid.worker.remote.v1.SidecarMessage;
import io.grpc.Grpc;
import io.grpc.InsecureChannelCredentials;
import io.grpc.ManagedChannel;
import io.grpc.stub.StreamObserver;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.time.Clock;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Keepalive of the node: a sidecar whose connection hangs (open, nothing passes) loses its session; a quiet but live
 * one keeps it. The sidecars here do not ping: only the pings of the server are checked. gRPC raises the keepalive
 * time to 10s, so the test takes ~11s.
 */
class NodeServerTest {
    private static final Duration WAIT = Duration.ofSeconds(20);

    private final RemoteWorker worker = new RemoteWorker(RemoteSettings.DEFAULTS, Clock.systemUTC());
    private final NodeServer server = new NodeServer(0, new NodeService(worker),
        new NodeServer.Keepalive(Duration.ofSeconds(10), Duration.ofMillis(500), Duration.ofSeconds(5)), Duration.ofSeconds(1));
    private final List<AutoCloseable> resources = new ArrayList<>();

    @AfterEach
    void stop() throws Exception {
        for (AutoCloseable resource : resources.reversed()) {
            resource.close();
        }
        server.stop();
        worker.close();
    }

    @Test
    void hungSidecarLosesItsSession() throws Exception {
        server.start();
        final FreezingProxy hung = new FreezingProxy(server.port());
        final FreezingProxy live = new FreezingProxy(server.port());
        connect(hung, "hung");
        connect(live, "live");
        await("both sessions open", () -> capacity("hung") == 1 && capacity("live") == 1);

        hung.freeze();

        await("hung session closed", () -> capacity("hung") == 0);
        // the live one answered the same pings
        Thread.sleep(500);
        assertEquals(1, capacity("live"));
    }

    private void connect(FreezingProxy proxy, String kind) {
        final ManagedChannel channel = Grpc.newChannelBuilderForAddress("localhost", proxy.port(), InsecureChannelCredentials.create())
            .build();
        resources.add(() -> channel.shutdownNow().awaitTermination(5, TimeUnit.SECONDS));
        final StreamObserver<SidecarMessage> session = NodeGrpc.newStub(channel).connect(new StreamObserver<>() {
            @Override
            public void onNext(NodeMessage message) {
            }

            @Override
            public void onError(Throwable error) {
            }

            @Override
            public void onCompleted() {
            }
        });
        session.onNext(SidecarMessage.newBuilder()
            .setHello(Hello.newBuilder().setWorkerId("worker-" + kind).setInstanceId("i").setKind(kind).setCapacity(1))
            .build());
    }

    private double capacity(String kind) {
        return worker.load().getOrDefault(kind, Load.EMPTY).capacity();
    }

    private static void await(String what, Check check) throws InterruptedException {
        final long deadline = System.nanoTime() + WAIT.toNanos();
        while (!check.ok()) {
            assertTrue(System.nanoTime() < deadline, "not within " + WAIT + ": " + what);
            Thread.sleep(20);
        }
    }

    private interface Check {
        boolean ok();
    }

    /**
     * TCP proxy to the node; frozen - keeps its connections open and passes nothing, like a hung network path.
     */
    private final class FreezingProxy {
        private final ServerSocket listener;
        private final CountDownLatch frozen = new CountDownLatch(1);
        private final CountDownLatch released = new CountDownLatch(1);

        FreezingProxy(int target) throws IOException {
            this.listener = new ServerSocket(0, 50, InetAddress.getLoopbackAddress());
            resources.add(() -> {
                released.countDown();
                listener.close();
            });
            Thread.ofVirtual().start(() -> accept(target));
        }

        int port() {
            return listener.getLocalPort();
        }

        void freeze() {
            frozen.countDown();
        }

        private void accept(int target) {
            try {
                while (true) {
                    final Socket in = listener.accept();
                    final Socket out = new Socket(InetAddress.getLoopbackAddress(), target);
                    resources.add(in);
                    resources.add(out);
                    Thread.ofVirtual().start(() -> pipe(in, out));
                    Thread.ofVirtual().start(() -> pipe(out, in));
                }
            } catch (IOException e) {
                // closed
            }
        }

        private void pipe(Socket from, Socket to) {
            final byte[] buffer = new byte[32 * 1024];
            try {
                final InputStream input = from.getInputStream();
                final OutputStream output = to.getOutputStream();
                int n;
                while ((n = input.read(buffer)) >= 0) {
                    if (frozen.getCount() == 0) {
                        released.await();
                        return;
                    }
                    output.write(buffer, 0, n);
                }
            } catch (IOException e) {
                // closed
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }
}
