package io.github.deripas.taskgrid.task;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.HexFormat;
import java.util.Objects;

/**
 * Opaque bytes of a task payload or result: inline in heap, or a reference to blob storage.
 * <p>
 * The type does no I/O: reading the content of a {@link Ref} is the job of a storage adapter.
 */
public sealed interface Blob {
    /**
     * Content held in heap. The array is not copied: callers must not modify it after passing it in.
     */
    record Inline(byte[] bytes) implements Blob {
        public Inline {
            Objects.requireNonNull(bytes, "bytes");
        }

        @Override
        public long size() {
            return bytes.length;
        }

        /**
         * Computed on each call, not cached.
         */
        @Override
        public String hash() {
            try {
                return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(bytes));
            } catch (NoSuchAlgorithmException e) {
                throw new IllegalStateException("SHA-256 is required of every JDK", e);
            }
        }

        @Override
        public boolean equals(Object o) {
            return o instanceof Inline other && Arrays.equals(bytes, other.bytes);
        }

        @Override
        public int hashCode() {
            return Arrays.hashCode(bytes);
        }

        @Override
        public String toString() {
            return "Inline[size=" + bytes.length + "]";
        }
    }

    /**
     * Content in blob storage.
     *
     * @param location storage-specific address of the object, e.g. a key in a bucket or a SeaweedFS fid
     * @param hash     SHA-256 of the content, as computed when it was stored
     */
    record Ref(
        String location,
        long size,
        String hash
    ) implements Blob {
        public Ref {
            Objects.requireNonNull(location, "location");
            Objects.requireNonNull(hash, "hash");
        }
    }

    /**
     * Size of the content in bytes.
     */
    long size();

    /**
     * SHA-256 of the content, lowercase hex: tells whether two blobs carry the same content.
     */
    String hash();

    static Blob of(byte[] bytes) {
        return new Inline(bytes);
    }
}
