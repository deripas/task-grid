package io.github.deripas.taskgrid.worker.remote;

import java.time.Duration;
import java.util.Objects;

/**
 * Settings of the remote worker.
 *
 * @param tick          how often waiting attempts widen their candidates and expire
 * @param affinityWait  an attempt with an affinity key waits this long for credit of the best sidecar by HRW,
 *                      then of the three best, after twice as long - of any
 * @param spreadWait    an attempt without an affinity key waits this long for credit of two random sidecars, then of any
 * @param assignTimeout no credit for this long - the attempt is rejected
 */
public record RemoteSettings(
    Duration tick,
    Duration affinityWait,
    Duration spreadWait,
    Duration assignTimeout
) {
    public static final RemoteSettings DEFAULTS = new RemoteSettings(
        Duration.ofMillis(10), Duration.ofMillis(200), Duration.ofMillis(20), Duration.ofSeconds(30));

    public RemoteSettings {
        positive(tick, "tick");
        positive(affinityWait, "affinityWait");
        positive(spreadWait, "spreadWait");
        positive(assignTimeout, "assignTimeout");
    }

    private static void positive(Duration value, String name) {
        Objects.requireNonNull(value, name);
        if (!value.isPositive()) {
            throw new IllegalArgumentException(name + " must be positive: " + value);
        }
    }
}
