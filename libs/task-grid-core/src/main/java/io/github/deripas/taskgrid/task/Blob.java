package io.github.deripas.taskgrid.task;

import java.util.Arrays;
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
     */
    record Ref(String location, long size) implements Blob {
        public Ref {
            Objects.requireNonNull(location, "location");
        }
    }

    /**
     * Size of the content in bytes.
     */
    long size();

    static Blob of(byte[] bytes) {
        return new Inline(bytes);
    }
}
