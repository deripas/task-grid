package io.github.deripas.crawler.urlmanager.protocol;

import io.github.deripas.crawler.urlmanager.fetch.LocalFetch;
import io.github.deripas.crawler.urlmanager.v1.FetchReply;
import io.github.deripas.crawler.urlmanager.v1.FetchRequest;
import io.github.deripas.crawler.urlmanager.v1.Refusal;
import io.github.deripas.taskgrid.gateway.InvalidPayloadException;
import io.github.deripas.taskgrid.queue.TaskQueue;
import io.github.deripas.taskgrid.task.Blob;
import io.github.deripas.taskgrid.task.Task;
import io.github.deripas.taskgrid.task.TaskResult;
import io.github.deripas.taskgrid.task.TaskStatus;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class FetchTaskCodecTest {
    private final LocalFetch fetch = new LocalFetch(attempt -> Blob.of(new byte[0]), ref -> {
        throw new IOException("storage is down");
    });
    private final FetchTaskCodec codec = new FetchTaskCodec(fetch.service());

    @AfterEach
    void stop() {
        fetch.close();
    }

    @Test
    void requestIsTheTaskOfItsUrl() throws InvalidPayloadException {
        final Task task = codec.task(FetchRequest.newBuilder().setRequestId("ignored").setUrl("https://a.com/x").build().toByteArray());

        assertEquals("a.com", task.meta().get(TaskQueue.PARTITION_KEY));
        assertEquals(task.id(), codec.task(FetchRequest.newBuilder().setUrl("https://a.com/x").build().toByteArray()).id(),
            "the same URL is the same task");
    }

    @Test
    void notARequestOrABadUrlIsInvalid() {
        assertThrows(InvalidPayloadException.class, () -> codec.task(new byte[]{1, 2, 3}));
        final byte[] ftp = FetchRequest.newBuilder().setUrl("ftp://a.com").build().toByteArray();
        assertThrows(InvalidPayloadException.class, () -> codec.task(ftp));
    }

    @Test
    void resultIsTheReplyToTheRequest() throws Exception {
        final TaskResult done = new TaskResult("t", TaskStatus.DONE, Blob.of("page".getBytes(StandardCharsets.UTF_8)), null, Map.of());

        final FetchReply reply = FetchReply.parseFrom(codec.reply("g-1", done));

        assertEquals("g-1", reply.getRequestId());
        assertEquals("page", reply.getPage().getBody().toStringUtf8());
    }

    @Test
    void pageThatCannotBeReadIsARefusalOfUrlManager() throws Exception {
        final TaskResult done = new TaskResult("t", TaskStatus.DONE, new Blob.Ref("s3://results/t", 5, "h", null), null, Map.of());

        final FetchReply reply = FetchReply.parseFrom(codec.reply("g-1", done));

        assertEquals(Refusal.Code.CODE_FAILED, reply.getRefusal().getCode());
    }
}
