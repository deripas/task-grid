package io.github.deripas.taskgrid.task;

import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

class BlobTest {
    @Test
    void hashIsSha256OfTheContent() {
        assertEquals("ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad", Blob.of(bytes("abc")).hash());
        assertEquals(Blob.of(bytes("abc")).hash(), Blob.of(bytes("abc")).hash());
        assertNotEquals(Blob.of(bytes("abc")).hash(), Blob.of(bytes("abd")).hash());
    }

    private static byte[] bytes(String text) {
        return text.getBytes(StandardCharsets.UTF_8);
    }
}
