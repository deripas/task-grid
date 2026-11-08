package io.github.deripas.crawler.urlmanager.protocol;

import com.google.protobuf.InvalidProtocolBufferException;
import io.github.deripas.crawler.urlmanager.fetch.FetchService;
import io.github.deripas.crawler.urlmanager.fetch.InvalidUrlException;
import io.github.deripas.crawler.urlmanager.fetch.PageUrl;
import io.github.deripas.crawler.urlmanager.v1.FetchRequest;
import io.github.deripas.crawler.urlmanager.v1.Refusal.Code;
import io.github.deripas.taskgrid.gateway.InvalidPayloadException;
import io.github.deripas.taskgrid.gateway.TaskCodec;
import io.github.deripas.taskgrid.task.Task;
import io.github.deripas.taskgrid.task.TaskResult;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * The protocol {@code crawler.urlmanager.v1} over the gateways of task-grid (gRPC, RabbitMQ): the payload of a request
 * is {@code FetchRequest}, the reply is {@code FetchReply} with the id of the request on its transport (the correlation
 * id of a message, the id of a gRPC request). {@code request_id} and {@code timeout_ms} of {@code FetchRequest} are not
 * read: the id of a request is given by its transport, the time it waits - by url-manager.
 */
@Slf4j
@RequiredArgsConstructor
public final class FetchTaskCodec implements TaskCodec {
    private final FetchService service;

    @Override
    public Task task(byte[] payload) throws InvalidPayloadException {
        final FetchRequest request;
        try {
            request = FetchRequest.parseFrom(payload);
        } catch (InvalidProtocolBufferException e) {
            throw new InvalidPayloadException("payload is not crawler.urlmanager.v1.FetchRequest: " + e.getMessage());
        }
        try {
            return service.task(PageUrl.parse(request.getUrl()));
        } catch (InvalidUrlException e) {
            throw new InvalidPayloadException(e.getMessage());
        }
    }

    /**
     * A page that cannot be read (blob storage) is a refusal of url-manager: {@code CODE_FAILED}.
     */
    @Override
    public byte[] refusal(
        String id,
        Refusal refusal,
        String message
    ) {
        final Code code = switch (refusal) {
            case INVALID -> Code.CODE_INVALID;
            case FAILED -> Code.CODE_FAILED;
            case TIMEOUT -> Code.CODE_TIMEOUT;
            case BUSY -> Code.CODE_BUSY;
        };
        return Replies.refusal(id, code, message).toByteArray();
    }

    @Override
    public byte[] reply(String id, TaskResult result) {
        try {
            return Replies.reply(id, service.outcome(result)).toByteArray();
        } catch (Exception e) {
            log.error("Finished fetch not read: request={}", id, e);
            return Replies.refusal(id, Code.CODE_FAILED, "url-manager: " + e).toByteArray();
        }
    }
}
