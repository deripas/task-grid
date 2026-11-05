package io.github.deripas.crawler.urlmanager.app;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import io.github.deripas.crawler.urlmanager.config.UrlManagerProperties;
import io.github.deripas.crawler.urlmanager.grpc.NodeServer;
import lombok.extern.slf4j.Slf4j;
import org.apache.pekko.actor.Address;
import org.apache.pekko.actor.CoordinatedShutdown;
import org.apache.pekko.actor.typed.ActorSystem;
import org.apache.pekko.actor.typed.javadsl.Behaviors;
import org.apache.pekko.cluster.typed.Cluster;
import org.apache.pekko.cluster.typed.JoinSeedNodes;
import org.springframework.context.SmartLifecycle;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * The actor system of the node in the Pekko cluster of url-managers. Created with the beans (the queue starts its
 * partitions on it), joins the cluster on start - after the gRPC server of the node, so the attempts of partitions
 * coming here find its sidecars - and leaves on stop before the gRPC server: partitions move to other nodes while
 * sidecars may still deliver results here.
 */
@Slf4j
final class PekkoCluster implements SmartLifecycle {
    static final String NAME = "url-manager";

    private final ActorSystem<Void> system;
    private final UrlManagerProperties.Cluster settings;
    private volatile boolean running;

    PekkoCluster(UrlManagerProperties.Cluster settings) {
        this.settings = settings;
        this.system = ActorSystem.create(Behaviors.empty(), NAME, config(settings));
    }

    ActorSystem<Void> system() {
        return system;
    }

    @Override
    public void start() {
        final List<Address> seeds = settings.seedNodes().isEmpty()
            ? List.of(Cluster.get(system).selfMember().address())
            : settings.seedNodes().stream().map(PekkoCluster::address).toList();
        Cluster.get(system).manager().tell(new JoinSeedNodes(seeds));
        running = true;
        log.info("Joining the cluster: self={} seeds={}", Cluster.get(system).selfMember().address(), seeds);
    }

    /**
     * Leaves the cluster (partitions hand off to other nodes) and terminates the actor system.
     */
    @Override
    public void stop() {
        try {
            CoordinatedShutdown.get(system).runAll(CoordinatedShutdown.unknownReason()).toCompletableFuture().get(30, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (Exception e) {
            log.warn("Cluster shutdown did not complete", e);
        }
        running = false;
    }

    @Override
    public boolean isRunning() {
        return running;
    }

    /**
     * After the gRPC server of the node (starts after it, stops before it).
     */
    @Override
    public int getPhase() {
        return NodeServer.PHASE + 1;
    }

    private static Config config(UrlManagerProperties.Cluster settings) {
        final Map<String, Object> deployment = new HashMap<>();
        deployment.put("pekko.remote.artery.canonical.hostname", settings.host());
        deployment.put("pekko.remote.artery.canonical.port", settings.port());
        deployment.put("datastax-java-driver.basic.contact-points", settings.contactPoints());
        deployment.put("datastax-java-driver.basic.load-balancing-policy.local-datacenter", settings.datacenter());
        return ConfigFactory.parseMap(deployment).withFallback(ConfigFactory.load());
    }

    private static Address address(String hostPort) {
        final int colon = hostPort.lastIndexOf(':');
        if (colon < 0) {
            throw new IllegalArgumentException("seed node is not host:port: " + hostPort);
        }
        return new Address("pekko", NAME, hostPort.substring(0, colon), Integer.parseInt(hostPort.substring(colon + 1)));
    }
}
