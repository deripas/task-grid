package io.github.deripas.crawler.urlmanager;

import com.github.tomakehurst.wiremock.client.WireMock;
import com.github.tomakehurst.wiremock.verification.LoggedRequest;
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import io.github.deripas.crawler.urlmanager.protocol.Replies;
import io.github.deripas.crawler.urlmanager.v1.FetchReply;
import io.github.deripas.crawler.urlmanager.v1.FetchRequest;
import io.github.deripas.taskgrid.gateway.grpc.v1.AckReply;
import io.github.deripas.taskgrid.gateway.grpc.v1.AckRequest;
import io.github.deripas.taskgrid.gateway.grpc.v1.Result;
import io.github.deripas.taskgrid.gateway.grpc.v1.SubmitRequest;
import io.github.deripas.taskgrid.gateway.grpc.v1.SubscribeRequest;
import io.github.deripas.taskgrid.gateway.grpc.v1.TaskGatewayGrpc;
import io.grpc.Grpc;
import io.grpc.InsecureChannelCredentials;
import io.grpc.ManagedChannel;
import io.grpc.stub.StreamObserver;
import org.junit.jupiter.api.Test;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.containers.ComposeContainer;
import org.testcontainers.containers.ContainerState;
import org.testcontainers.containers.wait.strategy.Wait;

import java.io.File;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static com.github.tomakehurst.wiremock.client.WireMock.anyRequestedFor;
import static com.github.tomakehurst.wiremock.client.WireMock.anyUrl;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The docker-compose stand ({@code crawler-example/stand}) of each profile, as a client sees it: requests go to the
 * consistent-hash exchange with the host as the routing key, every request gets its page, a host is fetched no more
 * often than the crawl-delay - while a url-manager is stopped in the middle of the work. Builds the images of the stand
 * and needs Docker with {@code docker compose}; run by {@code make stand-it}, not by {@code make it}.
 */
class StandIT {
    private static final Duration START = Duration.ofMinutes(5);
    private static final Duration WAIT = Duration.ofMinutes(3);
    // url-manager.crawl-delay of the stand (default of application.yaml); the clock is the one of the site
    private static final Duration CRAWL_DELAY = Duration.ofSeconds(1);
    private static final Duration JITTER = Duration.ofMillis(50);
    private static final List<String> HOSTS = List.of("site-a", "site-b", "site-c", "site-d", "site-e");
    private static final int PAGES = 8;
    // /large of the site: above INLINE_MAX of the sidecar, the page goes through blob storage
    private static final int LARGE = 600_000;
    private static final String EXCHANGE = "url-manager.fetch";
    private static final int AMQP_PORT = 5672;
    private static final int SITE_PORT = 8080;
    private static final int NODE_PORT = 9090;
    /**
     * Pages asked for over the gRPC gateway in the cluster profile.
     */
    private static final int GATEWAY_PAGES = 4;

    /**
     * Queues of their own behind the exchange: the instance with the most requests in work is restarted; its
     * unacknowledged requests wait in its queue and are fetched after the restart.
     */
    @Test
    void memoryProfileSurvivesARestartOfAnInstance() throws Exception {
        try (ComposeContainer stand = stand("memory")) {
            stand.start();
            crawl(stand, "rabbitmq-memory", () -> {
                final String busiest = busiestQueue(stand);
                final String service = "url-manager-memory-" + busiest.substring(busiest.lastIndexOf('.') + 1);
                final String id = container(stand, service).getContainerId();
                DockerClientFactory.instance().client().stopContainerCmd(id).withTimeout(40).exec();
                DockerClientFactory.instance().client().startContainerCmd(id).exec();
            });
        }
    }

    /**
     * One queue, partitions in the Pekko cluster: a node leaves for good; its partitions move to the other nodes with
     * their journal, results of attempts in progress come through the other nodes. A client of the gRPC gateway
     * submits to one node and takes its results from another: the boxes are in the cluster too.
     */
    @Test
    void clusterProfileSurvivesALostNode() throws Exception {
        try (ComposeContainer stand = stand("cluster")) {
            stand.start();
            final ManagedChannel first = channel(stand, "url-manager-cluster-1");
            final ManagedChannel third = channel(stand, "url-manager-cluster-3");
            try {
                final Map<String, String> expected = new ConcurrentHashMap<>();
                for (int i = 0; i < GATEWAY_PAGES; i++) {
                    final String host = HOSTS.get(i % HOSTS.size());
                    final String id = "g-" + i;
                    final String url = "http://" + host + ":" + SITE_PORT + "/gateway/" + i;
                    TaskGatewayGrpc.newBlockingStub(first).submit(SubmitRequest.newBuilder()
                        .setClient("pagenator").setConsumer("0").setId(id)
                        .setPayload(FetchRequest.newBuilder().setUrl(url).build().toByteString())
                        .build());
                    expected.put(id, "page " + host + "/gateway/" + i);
                }
                crawl(stand, "rabbitmq-cluster", () -> {
                    final String id = container(stand, "url-manager-cluster-2").getContainerId();
                    DockerClientFactory.instance().client().stopContainerCmd(id).withTimeout(40).exec();
                });
                assertGatewayResults(third, expected);
            } finally {
                first.shutdownNow();
                third.shutdownNow();
            }
        }
    }

    /**
     * The results of the box come on the other node; acknowledged, they do not come again.
     */
    private static void assertGatewayResults(ManagedChannel channel, Map<String, String> expected) throws Exception {
        final BlockingQueue<Result> results = new LinkedBlockingQueue<>();
        TaskGatewayGrpc.newStub(channel).subscribe(SubscribeRequest.newBuilder().setClient("pagenator").setConsumer("0").build(),
            new StreamObserver<>() {
                @Override
                public void onNext(Result result) {
                    results.add(result);
                }

                @Override
                public void onError(Throwable error) {
                }

                @Override
                public void onCompleted() {
                }
            });
        final Map<String, String> pages = new ConcurrentHashMap<>();
        final long deadline = System.nanoTime() + WAIT.toNanos();
        while (pages.size() < expected.size()) {
            final Result result = results.poll(Math.max(1, deadline - System.nanoTime()), TimeUnit.NANOSECONDS);
            assertTrue(result != null, "gateway results: " + pages.keySet() + " within " + WAIT);
            pages.put(result.getId(), FetchReply.parseFrom(result.getPayload()).getPage().getBody().toStringUtf8());
        }
        assertEquals(expected, pages);
        final StreamObserver<AckRequest> acks = TaskGatewayGrpc.newStub(channel).ack(new StreamObserver<>() {
            @Override
            public void onNext(AckReply reply) {
            }

            @Override
            public void onError(Throwable error) {
            }

            @Override
            public void onCompleted() {
            }
        });
        acks.onNext(AckRequest.newBuilder().setClient("pagenator").setConsumer("0").addAllIds(expected.keySet()).build());
        acks.onCompleted();
    }

    private static ManagedChannel channel(ComposeContainer stand, String service) {
        final String instance = instance(service);
        return Grpc.newChannelBuilderForAddress(stand.getServiceHost(instance, NODE_PORT), stand.getServicePort(instance, NODE_PORT),
            InsecureChannelCredentials.create()).build();
    }

    private static ComposeContainer stand(String profile) {
        final File compose = new File(System.getProperty("stand.dir", "../stand"), "compose.yaml");
        final ComposeContainer stand = new ComposeContainer(compose)
            .withEnv("COMPOSE_PROFILES", profile)
            .withBuild(true)
            .withExposedService("rabbitmq-" + profile, AMQP_PORT)
            .withExposedService("site", SITE_PORT)
            .waitingFor(instance("sidecar-1"), Wait.forLogMessage(".*sessions=3.*", 1))
            .waitingFor(instance("sidecar-2"), Wait.forLogMessage(".*sessions=3.*", 1))
            .withStartupTimeout(START);
        for (int i = 1; i <= 3; i++) {
            stand.waitingFor(instance("url-manager-" + profile + "-" + i), Wait.forLogMessage(".*Started UrlManagerApplication.*", 1));
        }
        if ("cluster".equals(profile)) {
            // the gRPC gateway of two nodes: submit to one, take the results from another
            stand.withExposedService(instance("url-manager-cluster-1"), NODE_PORT)
                .withExposedService(instance("url-manager-cluster-3"), NODE_PORT);
        }
        return stand;
    }

    /**
     * Sends every request, disrupts the stand after the first replies, waits for all the pages and checks the
     * crawl-delay in the journal of the site.
     */
    private static void crawl(
        ComposeContainer stand,
        String rabbit,
        Disruption disruption
    ) throws Exception {
        final Map<String, String> expected = new ConcurrentHashMap<>();
        for (String host : HOSTS) {
            for (int i = 0; i < PAGES; i++) {
                expected.put(host + "/p" + i, "page " + host + "/p" + i);
            }
        }
        expected.put("site-a/large", "");

        final ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(stand.getServiceHost(rabbit, AMQP_PORT));
        factory.setPort(stand.getServicePort(rabbit, AMQP_PORT));
        factory.setUsername("stand");
        factory.setPassword("stand");
        final Map<String, FetchReply> replies = new ConcurrentHashMap<>();
        try (Connection connection = factory.newConnection();
             Channel consumer = connection.createChannel();
             Channel publisher = connection.createChannel()) {
            final String replyTo = consumer.queueDeclare().getQueue();
            consumer.basicConsume(replyTo, true, (tag, delivery) -> {
                final FetchReply reply = FetchReply.parseFrom(delivery.getBody());
                replies.putIfAbsent(reply.getRequestId(), reply);
            }, tag -> {
            });
            for (String id : expected.keySet()) {
                final String host = id.substring(0, id.indexOf('/'));
                final AMQP.BasicProperties properties = new AMQP.BasicProperties.Builder()
                    .contentType(Replies.CONTENT_TYPE)
                    .replyTo(replyTo)
                    .correlationId(id)
                    .build();
                final String url = "http://" + host + ":" + SITE_PORT + id.substring(host.length());
                final FetchRequest request = FetchRequest.newBuilder().setRequestId(id).setUrl(url).build();
                publisher.basicPublish(EXCHANGE, host, properties, request.toByteArray());
            }

            await("first replies", () -> replies.size() >= 10);
            disruption.run();
            await("all replies", () -> replies.size() == expected.size());
        }

        expected.forEach((id, body) -> {
            final FetchReply reply = replies.get(id);
            assertTrue(reply.hasPage(), id + ": " + reply);
            assertEquals(200, reply.getPage().getStatus(), id);
            if (body.isEmpty()) {
                assertEquals(LARGE, reply.getPage().getBody().size(), id);
            } else {
                assertEquals(body, reply.getPage().getBody().toStringUtf8(), id);
            }
        });
        assertCrawlDelay(new WireMock(stand.getServiceHost("site", SITE_PORT), stand.getServicePort("site", SITE_PORT)));
    }

    /**
     * Requests of the site by host, in the order of arrival: two of a host at least the crawl-delay apart.
     */
    private static void assertCrawlDelay(WireMock site) {
        final Map<String, List<LoggedRequest>> byHost = site.find(anyRequestedFor(anyUrl())).stream()
            .collect(Collectors.groupingBy(r -> r.getHeader("Host").split(":")[0]));
        assertEquals(HOSTS, byHost.keySet().stream().sorted().toList());
        byHost.forEach((host, requests) -> {
            final List<LoggedRequest> sorted = new ArrayList<>(requests);
            sorted.sort(Comparator.comparing(LoggedRequest::getLoggedDate));
            for (int i = 1; i < sorted.size(); i++) {
                final long gap = sorted.get(i).getLoggedDate().getTime() - sorted.get(i - 1).getLoggedDate().getTime();
                assertTrue(gap >= CRAWL_DELAY.minus(JITTER).toMillis(),
                    host + ": " + sorted.get(i - 1).getUrl() + " and " + sorted.get(i).getUrl() + " " + gap + " ms apart");
            }
        });
    }

    /**
     * The request queue of the memory profile with the most messages (ready and in work).
     */
    private static String busiestQueue(ComposeContainer stand) throws Exception {
        final String out = container(stand, "rabbitmq-memory")
            .execInContainer("rabbitmqctl", "list_queues", "-q", "name", "messages")
            .getStdout();
        return out.lines()
            .map(line -> line.trim().split("\\s+"))
            .filter(columns -> columns.length == 2 && columns[0].startsWith(EXCHANGE + "."))
            .max(Comparator.comparingInt(columns -> Integer.parseInt(columns[1])))
            .map(columns -> columns[0])
            .orElseThrow(() -> new AssertionError("no request queues: " + out));
    }

    private static ContainerState container(ComposeContainer stand, String service) {
        return stand.getContainerByServiceName(instance(service)).orElseThrow(() -> new AssertionError("no service " + service));
    }

    /**
     * The first container of a service, as compose names it.
     */
    private static String instance(String service) {
        return service + "-1";
    }

    private static void await(String what, Check check) throws InterruptedException {
        final long deadline = System.nanoTime() + WAIT.toNanos();
        while (!check.done()) {
            assertTrue(System.nanoTime() < deadline, what + ": not within " + WAIT);
            Thread.sleep(200);
        }
    }

    @FunctionalInterface
    private interface Disruption {
        void run() throws Exception;
    }

    @FunctionalInterface
    private interface Check {
        boolean done();
    }
}
