package io.github.deripas.taskgrid.worker.remote;

/**
 * Rendezvous hashing: the preferred sidecar of a key is the one with the highest score(key, worker).
 * When a sidecar leaves, only its keys move to the next ones.
 */
final class Hrw {
    private Hrw() {
    }

    /**
     * Compare scores with {@link Long#compareUnsigned}.
     */
    static long score(String key, String worker) {
        long h = 0xcbf29ce484222325L;
        h = fnv(h, key);
        h = (h ^ 0xff) * 0x100000001b3L;
        h = fnv(h, worker);
        // splitmix64 finalizer: FNV mixes the high bits poorly
        h = (h ^ (h >>> 30)) * 0xbf58476d1ce4e5b9L;
        h = (h ^ (h >>> 27)) * 0x94d049bb133111ebL;
        return h ^ (h >>> 31);
    }

    private static long fnv(long seed, String s) {
        long h = seed;
        for (int i = 0; i < s.length(); i++) {
            h = (h ^ s.charAt(i)) * 0x100000001b3L;
        }
        return h;
    }
}
