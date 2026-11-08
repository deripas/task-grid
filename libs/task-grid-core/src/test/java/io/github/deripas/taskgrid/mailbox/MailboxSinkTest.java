package io.github.deripas.taskgrid.mailbox;

import io.github.deripas.taskgrid.mailbox.memory.MemoryMailbox;
import io.github.deripas.taskgrid.task.Request;
import io.github.deripas.taskgrid.task.RequestId;
import io.github.deripas.taskgrid.task.TaskResult;
import io.github.deripas.taskgrid.task.TaskStatus;
import org.junit.jupiter.api.Test;

import java.time.Clock;
import java.time.Duration;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

class MailboxSinkTest {
    private final MemoryMailbox mailbox = new MemoryMailbox(new MailboxSettings(10, Duration.ofMinutes(1)), Clock.systemUTC());
    private final MailboxSink sink = new MailboxSink(mailbox);

    @Test
    void resultGoesToTheBoxesOfTheMarkedRequestsOnly() {
        final RequestId marked = new RequestId("pagenator", "0", "r1");
        final RequestId other = new RequestId("pagenator", "0", "r2");
        final TaskResult result = new TaskResult("work", TaskStatus.DONE, null, null, Map.of());

        sink.finished(result, List.of(MailboxSink.request(marked), Request.of(other))).toCompletableFuture().join();

        assertEquals(Reservation.DONE, mailbox.reserve(marked).toCompletableFuture().join());
        assertEquals(Reservation.NEW, mailbox.reserve(other).toCompletableFuture().join());
    }
}
