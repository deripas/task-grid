package io.github.deripas.taskgrid.mailbox;

import io.github.deripas.taskgrid.queue.ResultCallback;
import io.github.deripas.taskgrid.task.Request;
import io.github.deripas.taskgrid.task.RequestId;
import io.github.deripas.taskgrid.task.TaskResult;
import lombok.RequiredArgsConstructor;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

/**
 * Puts the results of the requests made by {@link #request} into their boxes. Register it on every node with the queue
 * ({@code TaskQueue.onResult}): a result finishes on the node of its partition, the mark travels with the request.
 * Requests of other ways (a reply address of their own, e.g. {@code reply_to} of AMQP) are left to their callbacks.
 */
@RequiredArgsConstructor
public final class MailboxSink implements ResultCallback {
    /**
     * Tag of a request whose result goes to its box.
     */
    public static final String TAG = "mailbox";

    private final Mailbox mailbox;

    /**
     * A request whose result waits in the box of its client and consumer.
     */
    public static Request request(RequestId id) {
        return new Request(id, Map.of(TAG, "true"));
    }

    @Override
    public CompletionStage<Void> finished(TaskResult result, List<Request> requests) {
        final List<CompletableFuture<Void>> stages = new ArrayList<>(requests.size());
        for (Request request : requests) {
            if (request.tags().containsKey(TAG)) {
                stages.add(mailbox.put(request.id(), result).toCompletableFuture());
            }
        }
        return CompletableFuture.allOf(stages.toArray(CompletableFuture[]::new));
    }
}
