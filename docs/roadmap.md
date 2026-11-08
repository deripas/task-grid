# Roadmap

Moving the core over from the prototype (`crawler-maket/services/dispatch`) in small steps, not 1:1. Each step:
discussion, changes, `make verify`, commit. Status: `[ ]` not started, `[~]` in progress, `[x]` done, `[-]` skipped.
Agreed decisions - [decisions.md](decisions.md), layout - [structure.md](structure.md).

1. `[x]` **Name and structure.** Coordinates, own root parent, module layout, `Makefile`.
2. `[x]` **Concepts and vocabulary.** Task / attempt, kind, affinity key, worker / handler, statuses, blob; core packages;
   in-process vs remote handler. Result - [concepts.md](concepts.md).
3. `[x]` **task-grid-core: contracts.** Domain types and ports, `ArchitectureTest`.
4. `[x]` **task-grid-core: in-memory MVP.** Queue per affinity key, result store, in-process worker and handler;
   one JVM runs the whole flow.
5. `[x]` **Remote worker.** `task-grid-worker-remote`: protocol `taskgrid.worker.remote.v1` (one bidi session per
   sidecar), credits, HRW affinity; in-process gRPC test with a Java sidecar. Lease, large results - later.
6. `[x]` **handler-sidecar (Go).** One session with one node, credit, raw-codec handler call, results until OK,
   drain by half-close; arch-go rules; `make verify` covers Go too.
7. `[x]` **Java - Go smoke test.** `SidecarCompatibilityIT`: a node with `RemoteWorker` and the real `handler-sidecar`
   image (Testcontainers) in front of a test handler - results with meta, handler error, drain on SIGTERM; `make it`.
8. `[x]` **Fetcher.** Unary `fetcher.proto` with code generation in `fetcher-api`; `fetcher` - plain Java process, gRPC
   server on virtual threads over Apache HttpClient 5, failures as gRPC statuses; tests on WireMock.
9. `[x]` **Large results.** The sidecar takes handler responses up to 100 MiB (a sitemap.xml) and puts those above
   `INLINE_MAX` into S3 (zstd); `Complete` carries a `BlobRef`, the node gets `Blob.Ref`; reading it is an adapter's job.
   SeaweedFS in `SidecarCompatibilityIT`.
10. `[-]` **Starter.** Skipped: applications wire the libraries themselves for now; back when a second application
   needs the same wiring (see Later).
11. `[x]` **url-manager: synchronous `POST /fetch`.** Spring Boot application on the in-memory core (queue by affinity
   key = host, one fetch per host at a time, no delay yet), remote worker for the `fetch` kind, answer like a proxy,
   large pages streamed from S3; end to end url-manager - sidecar - fetcher - site with SeaweedFS (`UrlManagerIT`).
12. `[x]` **Crawl-delay.** `task-grid-worker-delayed`: a worker decorator holds the next attempt of a partition key
   back until a delay after the result of the previous one (finish to start); url-manager - 1 s per host. Delay from
   robots.txt, cooldown after 429 - through `DelayPolicy`, later. Requirements -
   `crawler-maket/domain-queue-requirements.md`. Moved into the queue in 14a
   ([D24](decisions.md#d24-queue-and-worker-are-decoupled-dispatcher-connects-them)), the module is gone.
13. `[x]` **url-manager: RabbitMQ with reply_to.** Typed client protocol (`url_manager.proto`, at first a module `url-manager-api`), Spring AMQP; two ways
   of handling by `ack`: `after-submit` (target: acknowledged once queued, the reply from the result callback of the
   queue, `TaskQueue.onResult`) and `after-reply` (transitional, for the in-memory queue: acknowledged after the
   reply is confirmed; requests of a host sharded to one instance by a consistent-hash exchange).
14. `[ ]` **Pekko engine.** Clustered url-manager (queue partitions by host across nodes), deployable for load tests.
   - `[x]` **14a. Queue and worker decoupled.** `TaskQueue` hands out ready attempts (`onReady`) and takes their
     results (`complete`); `Worker` reports to `onComplete`; `Dispatcher` (core) connects them. Crawl-delay is a
     pause of the partition (`DelayPolicy` in `queue`), `task-grid-worker-delayed` removed. Pool metrics by kind
     from the remote worker (`/actuator/prometheus`, for KEDA); no credit within assign-timeout - RETRY.
   - `[x]` **14b. Persistent queue on Pekko.** `Partition` is an event-sourced state machine in core
     (`queue.partition`, time as a parameter, attempt timeout), the in-memory queue runs on it;
     `task-grid-queue-pekko`: a partition is a sharded event-sourced entity (remembered, snapshots, timers for
     pauses and deadlines), `submit` completes after persist, `onResult` delivers until success; the meta of the
     task comes back with the result (`AttemptResult.meta`, `Complete.meta`) to find the partition on any node.
     Tests on an in-memory journal; the Cassandra / ScyllaDB plugin comes with url-manager in 14c.
   - `[x]` **14c. Clustered url-manager.** Requests over RabbitMQ (`after-submit`) in a cluster of url-managers,
     docker-compose stand.
     - `[x]` **14c-1. Cluster mode.** `url-manager.queue: memory | cluster`; cluster - `PekkoTaskQueue`, journal in
       ScyllaDB (`pekko-persistence-cassandra`), seed nodes, SBR; `after-submit` only, `POST /fetch` off. A result is
       taken only once the queue has it (`complete` returns a stage, `Complete` is OK after the journal).
       `url-manager-api` folded into url-manager.
     - `[x]` **14c-2. Sidecar sessions with every node.** DNS of `NODE_ADDRESS` -> a session per address, shared credit,
       capacity share for the metrics. Attempts in progress belong to the sidecar process (`instance_id`), not to
       the session: a reconnect keeps them, a lost sidecar's attempts go to other sidecars after `rejoin-timeout`
       without a queue retry; results go to the node of the attempt, to another node when it is gone (D29).
     - `[x]` **14c-3. docker-compose stand.** `crawler-example/stand`, profiles `memory` (queue per instance behind
       a consistent-hash exchange, `after-reply`) and `cluster` (Pekko + ScyllaDB, one queue, `after-submit`); sidecars
       + fetchers as pods, SeaweedFS, a fake site. `StandIT` (`make stand-it`): a url-manager stopped in the middle of
       work, every page comes, crawl-delay holds per host (D30).
15. `[ ]` **Client gateway.** Clients submit tasks over their transport and get the results on any node of the
   cluster; a box of a consumer holds its tasks until it acknowledges the results, at most a limit of them (D31).
   - `[x]` **15a-1. Mailbox.** Port `Mailbox` in core on `RequestId` (`reserve` / `release` / `put` / `subscribe` /
     `ack`, box = client + consumer, limit and ttl), `MemoryMailbox`.
   - `[x]` **15a-2. Queue keeps requests, mailbox keeps results.** `Request` (`RequestId` + tags), task id = hash of
     the payload, requests of the same task join it (`Joined`); `Started` / `Ended` without payload and result; the
     final result is delivered before `Ended` (Pekko: stash, `delivery-timeout`), no outbox; `ResultStore` removed -
     url-manager waits through boxes of its node, AMQP after-submit replies by `RequestId` (D32).
   - `[x]` **15a-3. gRPC gateway.** `libs/task-grid-gateway-grpc`: `TaskGateway` (`Submit`, `Subscribe` - a stream
     of results of a box, `Ack` - a stream of request ids), payload and reply - bytes of the application through its
     `TaskCodec`; `Gateway` (reserve -> submit, release on failure), `MailboxSink` in core puts the results of marked
     requests into their boxes on any node; url-manager reserves its waits too (`box-limit`, `Busy`).
   - `[x]` **15a-4. url-manager over gRPC.** `FetchTaskCodec` (`FetchRequest` -> the task of its URL, the result ->
     `FetchReply` built by `protocol.Replies`, shared with AMQP), `GatewayService` on the node port next to the
     sidecars (memory mode; cluster - with the mailbox on Pekko), `UrlManagerIT.pagesOverTheGateway`.
   - `[x]` **15a-5. Mailbox on Pekko.** `task-grid-pekko-core` (shared serialization), `task-grid-mailbox-pekko`
     (a box is a sharded event-sourced entity, a subscriber follows it by heartbeat), cluster mode of url-manager with
     the gateway, `POST /fetch` and `after-reply`, a gRPC client in `StandIT` (D34).
   - `[x]` **15b. AMQP gateway.** `Gateway`, `TaskCodec` (with refusals), `Call` - in core (`gateway`): the whole way
     from a client to a worker; `task-grid-gateway-amqp` on amqp-client (after-reply and after-submit, publisher
     confirms, automatic recovery), the id of a request - the hash of its message; url-manager gives the codec of
     `crawler.urlmanager.v1` and the Spring lifecycle (D35).
16. `[ ]` **Later.** Spring Boot starter, more metrics (queue, attempts).
