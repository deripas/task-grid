# Decisions

Short log of agreed decisions: what was decided and why. Newest last. A decision changes only with a new entry
that names the one it replaces.

## D1. Project name and coordinates

`task-grid`: a task management core with a distributed worker pool; crawling is one of its applications.
groupId `io.github.deripas` (Maven Central grants new namespaces only as `io.github.<user>`),
base Java package `io.github.deripas.taskgrid`. Maven module directory = artifactId.

## D2. Own root parent, Spring only where needed

The root pom is our own parent (versions, plugins), not `spring-boot-starter-parent`: libraries must not have
their dependency versions pinned by Spring Boot. The Spring Boot BOM is imported by Spring-based aggregators only
(`crawler-example/`, later `starters/`). Spring Modulith is not used; module boundaries are checked by ArchUnit.
A `task-grid-bom` for consumers is added once there are several published artifacts.

## D3. One core library

`task-grid-core` holds the whole domain: types, contracts (ports) and in-memory implementations, JDK only,
split into packages by area (task, queue, result, worker, ...). Goal: a complete MVP inside one JVM from core alone.
Further libraries are added per engine or transport, not per domain area.

## D4. Pekko is the target engine, not a core dependency

`url-manager` in its final form is clustered on Pekko (demo to the team as a clusterable alternative, deployable for
load tests). Pekko lives in a separate library; core and the in-memory implementations work without it, so another
engine can replace it. Pekko is introduced after the single-node flow works end to end.

## D5. Worker is an interface; remote handlers via a sidecar

`Worker` is a core interface; one implementation calls handlers inside the JVM, another sends attempts over gRPC
to `handler-sidecar` processes deployed next to remote handlers. The sidecar owns the non-trivial protocol logic
(credits, lease extension, completion retries, large results, drain), so handlers in any language (Java, Node.js)
stay minimal: one unary gRPC method. The sidecar is written in Go: small static binary, fast start, low footprint
next to the handler. The prototype called it `agent`.

## D6. Client protocol belongs to the application

`url-manager` includes the front (client-facing) component: its RabbitMQ and REST messages are crawler-specific and
typed, independent of the worker (fetcher) protocol. The application translates client requests into tasks;
reusable concerns (deduplication, limits, waiting for results) are library code.

## D7. Language

Everything in git (code, comments, docs, CLAUDE.md) is in English. Discussion happens in Russian.
Working notes in `.local/` may be in any language.

## D8. Minimal now, extend when needed

Types and contracts get only the fields and operations the current step uses. Everything else from the prototype
(leases, correlation, timings, attributes, rate limiting, deduplication, ...) is added by the step that needs it.
Growth of structures and code should follow growth of the requirements and stay visible in history.

## D9. Vocabulary

Task (not request) is the unit submitted by a client; an attempt is one execution of a task. `Worker` is the core
port that executes attempts (the prototype's `Dispatch`); a `Handler` does the actual work for a kind, in-process or
remote behind `handler-sidecar`. Completion comes back through `AttemptCallback`, not a future: in a cluster the
callback reaches the task's owner on any node. Attempt status and task status are separate enums.
`Blob` is bytes inline or a reference to storage; the domain type does no I/O. Details - [concepts.md](concepts.md).

## D10. Component properties travel in meta

`Task` has only what every component needs: `id`, `payload`, `meta` (`Map<String, String>`). Properties used by
particular components - kind (worker), affinity key (queue, remote worker), attempts (queue, in the result) - are
meta keys declared as constants by the component that owns them, with typed access in that component only.
New properties add a key, not a field. The whole task meta is passed to the handler and copied into `TaskResult`:
correlation data of the client (e.g. RabbitMQ `reply_to`, `correlation_id`) comes back with the result on any node.
Values that drive core logic stay typed fields: attempt number, statuses, error.

## D11. Minimal dependencies, not JDK only

Every component may use Lombok and `slf4j-api` (`@Slf4j`). Other libraries are added when needed, the minimum
necessary; for core the allowed list is the dependency rule in its `ArchitectureTest`, changed deliberately.

## D12. Where implementations live

Implementations useful beyond tests (in-memory queue and store, in-process worker - single-node mode, first steps of
url-manager) live in main code. Code needed only by tests (assembling a node, fakes) lives in tests.

## D13. Retry is opt-in; resubmission is recognized by payload hash

A handler failure is final unless the handler says otherwise: `RetryableException` means `RETRY`, any other exception
`FAILED`. Unknown failures are not retried blindly. `Blob` carries the SHA-256 of its content. A task submitted again
with an id that is still in the queue is the same task if the payload hash matches (a client retry - accepted, not
queued twice) and an error otherwise (the id is taken by another request).

## D14. Remote worker: one bidi session, retry on session loss, affinity by the key

- The sidecar and the node talk over `taskgrid.worker.remote.v1.Node`: `Connect` is one bidi stream per session
  (sidecar: `Hello`, `Credit`; node: `Attempt`, `Demand`), `Complete` is unary and repeated by the sidecar until OK.
  The prototype used two streams (`Connect` + `Credits`) glued by a session id in early headers; one stream needs neither.
- Credit and demand are taken from the prototype as is: absolute `(limit, version)` and `(target, version)`,
  demand in steps of capacity x 2^k, surplus credit returned by `Demand`.
- No lease yet: when a session ends, its attempts in progress are retried (`RETRY`, the attempt counts). A late result
  of such an attempt is ignored by the queue. Cost - a repeated execution after a network break. Lease (`Extend`,
  deadline) and incarnation come with the cluster, where an attempt must outlive a node; graceful stop - D15.
- Affinity follows the task: with `affinity-key` in meta - HRW over the sidecars of the kind, widened after
  affinity-wait; without it - any sidecar. No per-kind rule in settings. (Separated from the partition of the queue -
  D23.)
- Payloads are sent inline only; a `Blob.Ref` payload is rejected until blob storage arrives.
- The engine without Pekko is a single-thread executor around a pure state machine (`Allocator`), which the Pekko
  engine will reuse.

## D15. handler-sidecar: half-close is drain, single node for now

- The sidecar stops by half-closing its `Connect` stream: the node sends no more attempts to the session, waits for
  the results of its attempts in progress and closes the stream with OK. Cancellation of the call still ends the
  session at once, its attempts are retried. No separate `Drain` RPC.
- One session with one node (`NODE_ADDRESS`). Sessions with every node of a cluster (DNS), lease and restart detection
  come with the cluster engine; the credit ledger already distributes slots among several sessions.
- The handler gets the whole task meta as `x-meta-<key>` metadata. Handler statuses map to `DONE` / `FAILED` / `RETRY`
  only; `RESOURCE_EXHAUSTED` and `ABORTED` are `RETRY` until `RATE_LIMITED` / `RELEASED` exist.
- Results above `MAX_RESULT` (4 MiB, the inbound limit of the Java gRPC server) fail until blob storage exists
  (superseded by D19).
- Go module `github.com/deripas/task-grid/handler-sidecar`; code from `remote.proto` is generated with `protoc` and plugins
  from `tools.mod` and committed, so a plain `go build` needs no protoc.

## D16. Java - Go compatibility is tested against the image

`SidecarCompatibilityIT` runs the real `handler-sidecar` image (built from its `Dockerfile` by Testcontainers) between
a node and a test handler of the test JVM; the container reaches them through `Testcontainers.exposeHostPorts`.
It checks the artifact that is deployed, including drain on SIGTERM (`docker stop`). It needs Docker, so it runs in
`make it`, not in `make verify`; each side keeps its fast tests against a fake of the other one.

## D17. Fetcher: unary, failures as gRPC statuses, no Spring

- `crawler.fetcher.v1.Fetcher/Fetch` is unary (the sidecar convention): `url` -> `status`, `headers`, `body`. Fields are
  added when the crawler needs them (final URL, user agent, proxy, ...).
- Every response of the site, 404 and 500 included, is a result (`OK` -> `DONE`). Not an absolute http(s) URL -
  `INVALID_ARGUMENT`; the site cannot be reached (DNS, connect, TLS, timeout, broken response, redirect loop) -
  `FAILED_PRECONDITION`; body above `MAX_BODY` - `OUT_OF_RANGE`: all final, no retry. Only a failure of the fetcher
  itself is `INTERNAL` -> `RETRY`.
- `MAX_BODY` is 100 MiB (a sitemap.xml): above the 4 MiB result limit of the sidecar until large results go to blob
  storage (roadmap step 9).
- A plain Java process (gRPC server on virtual threads, Apache HttpClient 5 classic, no retries, no cookies), without
  Spring: a handler behind the sidecar is meant to be small and quick to start.

## D18. Stop order instead of a stop command for handlers

In a pod both containers get SIGTERM at once, but the handler must outlive the drain of its sidecar. The order is
set by the deployment, not by the protocol: in Kubernetes 1.29+ the handler is a native sidecar (init container with
`restartPolicy: Always`) and handler-sidecar the main container, so the handler starts first and stops last; in
docker-compose the sidecar `depends_on` the handler. A handler only has to shut down gracefully on SIGTERM - no
`close` command, which would be one more contract for every handler. Timeouts: `HANDLER_TIMEOUT < DRAIN_TIMEOUT <=`
shutdown timeout of the handler, `DRAIN_TIMEOUT` + margin `< terminationGracePeriodSeconds`. Details -
[handler-sidecar/README.md](../handler-sidecar/README.md#deployment-and-stop-order).

## D19. Large results: the sidecar puts them into S3, the node gets a reference

- Two limits of the sidecar: `MAX_RESPONSE` (100 MiB) - what it takes from the handler, above it `FAILED`;
  `INLINE_MAX` (512 KiB, at most 4 MiB - the inbound limit of the node) - above it the answer goes to blob storage and
  `Complete` carries `BlobRef{location, size, sha256, encoding}` instead of the body (`oneof result`). Replaces
  `MAX_RESULT` of D15. The node maps it to `Blob.Ref`; the core does not read it, an adapter of the application does.
- Storage - any S3 (minio-go): SeaweedFS in the tests, as in the prototype. The object is compressed with zstd
  (always: results are mostly text); size and sha256 are of the answer before compression, so a reader checks what
  it has decoded. `location` is `s3://<bucket>/<key>`: a reader needs only the endpoint and credentials.
- Key `<BLOB_PREFIX><task id>/<random>`: attempts do not overwrite each other, a repeat of `Complete` carries the same
  reference. The prefix is a setting of the sidecar; objects are deleted by a lifecycle rule (TTL) of the bucket on
  that prefix, set up with the bucket - not by the sidecar. A prefix per task from the node (different TTL per
  client) - when needed.
- The put happens within the slot of the attempt: memory of the sidecar stays bounded by `CAPACITY` answers.
  No storage configured and an answer above `INLINE_MAX` - `FAILED` (configuration does not change between attempts);
  the storage fails - `RETRY`.

## D20. url-manager: POST /fetch answers like a proxy, large pages are streamed

- `POST /fetch {"url", "timeoutMs"}` (JSON: room for fields such as a request id or priority). The site answered -
  200 with the body of the page, its `Content-Type` and the status of the site in `X-Fetch-Status` (404 of the site
  is a page); fetch failed - 502, no page within the timeout - 504, bad URL or timeout - 400; errors are problem
  details. Not JSON with a base64 body: a sitemap of 100 MiB would grow by a third and sit in memory.
- One task per fetch: id - random UUID, kind `fetch`, affinity key - the host in lower case (one fetch of a host at
  a time, the unit of politeness for crawl-delay, roadmap step 12); payload - `FetchRequest`.
- The page is a stream end to end: `FetchResponse` is read field by field (status, headers, then the body as the
  rest of the stream - the highest field number, written last), a `Blob.Ref` is read from S3 through zstd, and size
  and SHA-256 are checked as the body is sent (a mismatch breaks the response).
- S3 client - AWS SDK v2 with HttpClient 5 (`apache5-client`, its other HTTP clients excluded): minio-java brings
  okhttp with Kotlin, guava, jackson 2 and bouncycastle. Path-style addressing (SeaweedFS, MinIO); no keys - the
  default AWS credentials.
- No starter: `UrlManagerConfiguration` wires the in-memory node (`MemoryTaskQueue`, `MemoryResultStore`,
  `RemoteWorker`), components are not scanned. The gRPC server of the node starts before the web server and stops
  after it, cancelling the sessions: by then nobody waits for results.

## D21. Crawl-delay is a worker decorator, not a queue

_Superseded by [D24](#d24-queue-and-worker-are-decoupled-dispatcher-connects-them): the pause is part of the
partition. The rules of the pause below still hold, except the one about rejected assignments._

- The queue already runs one attempt of an affinity key at a time, in order; politeness only adds a pause between
  the result of an attempt and the next attempt of its key (finish to start). `DelayedWorker` (module
  `task-grid-worker-delayed`) wraps any worker and holds that next attempt back; queues stay as they are, and the
  same decorator serves the cluster engine on the node of the partition. A queue of its own would repeat retries,
  deduplication and attempt limits of `Partition`.
- Every result starts the pause - DONE, FAILED, RETRY (each was a request to the site), retries wait too. A rejected
  assignment made no request: no pause. Tasks without an affinity key and the first attempt of a key are not held.
  A key is kept only while its pause lasts.
- The pause comes from the port `DelayPolicy.after(key, result)`: fixed for now (`FixedDelay`, url-manager
  `crawl-delay: 1s`, including RETRY after a lost session); the result is passed so that the application can later
  take robots.txt and cooldown after 429 into account without changes of the library.

## D22. RabbitMQ: two ways of handling requests, chosen by the acknowledgement

- **Target - `url-manager.rabbit.ack: after-submit`** (`AckAfterSubmitRequests`): a request is acknowledged as soon
  as its task is queued; the reply address (reply_to, correlation_id) travels in the task meta, and the reply is
  sent by the result callback of the queue (`TaskQueue.onResult`) on the node where the task finished - any
  url-manager, whether or not the one that took the request is alive. Needs a queue that keeps its tasks and
  delivers results again until the reply is confirmed (Pekko); no timeout - the reply comes when the task finishes.
- **Transitional - `after-reply`, the default** (`AckAfterReplyRequests`): for the in-memory queue. A request waits
  for its page on the url-manager that took it and is acknowledged after its reply is confirmed by the broker
  (publisher confirms); a stop of url-manager or an unconfirmed reply - the request is delivered again and fetched
  again (at least once). The queues of instances are not shared, so the requests of a host must come to one
  instance: a consistent-hash exchange of RabbitMQ by host (infrastructure, not code). The prefetch of the consumer
  bounds the requests in work per instance, including those waiting for the crawl-delay of their host, so a hot host
  can take all of it (known, no limit per host yet); the wait must stay below `consumer_timeout` of the broker.
- Two implementations behind `FetchRequests`, chosen by a setting rather than by a property of the queue: the
  behaviour depends on the infrastructure too (sharding by the exchange), and during the move both must run on the
  same code; the transitional one is dropped as a whole later.
- Typed protocol `crawler.urlmanager.v1` (module `url-manager-api`): `FetchRequest{request_id, url, timeout_ms}` ->
  `FetchReply{request_id, page | refusal}`. The page body goes in the reply as is, up to the 100 MiB of the fetcher -
  `max_message_size` of the broker must allow it (RabbitMQ 4: 16 MiB by default); the production url-manager already
  passes such volumes through RabbitMQ.
- The request id (client's `request_id`, else `message_id`) is the task id: a request delivered again while its task
  is queued is not queued twice. A request without reply_to is acknowledged and dropped with a warning; a request
  that is not `FetchRequest` or has a bad URL gets `CODE_INVALID`.
- Spring AMQP of Spring Boot (`spring.rabbitmq.*`), manual acknowledgement on the channel of the delivery, from
  virtual threads.

## D23. Partition key and affinity key are separate

- One key had two roles that differ by kind. `partition-key` (`TaskQueue.PARTITION_KEY`) - correctness: tasks of a key
  run one at a time, in order, with the crawl-delay of `DelayedWorker`; every kind of the crawler sets it to the host.
  `affinity-key` (`Worker.AFFINITY_KEY`) - an optimization of a pool: HRW to the same sidecar for warm connections;
  `fetch` sets it to the host, renderers that start a browser per page do not (any free instance, no wait).
- The submitter sets them per task: it chooses the kind anyway. Not a per-kind setting of the node (one name would
  still mean two things, and the setting would live away from whoever knows the pool), not yet a flag in `Hello` of
  the sidecar - possible later on top of the key.
- Each key is declared by the component it belongs to. Replaces the single `affinity-key` of D14. (`DelayedWorker`,
  which read the partition key from the `queue` package, is gone with D24: the pause lives in the queue now.)

## D24. Queue and worker are decoupled, Dispatcher connects them

- Each implementation used to do the wiring itself: the queue created its worker with itself as the callback
  (`Function<AttemptCallback, Worker>`), and `DelayedWorker` did the same. The queue could not be used without a
  worker, and every engine (Pekko next) would repeat the wiring.
- Symmetric ports with subscription. `TaskQueue`: `submit`, `onReady(ReadyCallback)` - attempts ready to run,
  `complete(AttemptResult)`, `onResult`. `Worker`: `assign`, `onComplete(AttemptCallback)`. Either is usable alone:
  a client may take ready attempts and complete them itself. `Dispatcher.connect(queue, worker)` in core - the same
  for every implementation; an attempt the worker rejects is a FAILED result through the queue
  (`rejected: <reason>`), so the queue does not know `Assignment`.
- Not a pull API (`take` / lease): it needs a wake-up signal and lease timeouts and, in a cluster, a `take` to every
  shard, while the partition already tracks its attempt in progress. Not a function applied to the queue state
  (`process(key, state -> ...)`): a lambda cannot travel to the node of a partition nor go into a journal - commands
  must be data.
- Crawl-delay is a pause of the partition (`Partition` asks its engine to `resume` it after `DelayPolicy.after`): the
  queue owns the order of a key, so it owns the time between its attempts; the in-memory engine uses a timer, the
  Pekko engine will use timers of the entity. Rules of D21 hold; a rejected attempt is a FAILED result now and starts
  the pause too (rare: no handler for the kind, node stopping). A paused partition with no tasks stays until its
  pause ends.
- The buffer between the speed of the queue and the speed of the pool stays in the worker: the remote worker needs
  the waiting attempts to compute demand for the sidecars (credit, HRW widening). The queue hands out at most one
  attempt per partition, so the buffer is bounded by the active partitions.

## D25. Pool metrics come from the remote worker; no credit in time is a RETRY

- `RemoteWorker.load()` by kind: `waiting` (attempts waiting for a free slot - tasks idle because the pool is small),
  `busy` (attempts in progress), `capacity` (slots of the sessions). url-manager exports them as
  `task_grid_worker_{waiting,busy,capacity}{kind}` on `/actuator/prometheus` (Micrometer); KEDA scales the pools:
  throughput-bound pools (`fetch`, js-render) keep `busy / capacity` near a target (80%), latency-bound ones
  (page-speed) scale up as soon as `waiting > 0`.
- In a cluster each node reports its own view; `waiting` and `busy` add up, `capacity` of a sidecar connected to every
  node does not. Plan for 14c: the sidecar tells each node its share, `capacity / sessions` (updated when the number
  of its sessions changes), so the sum over the nodes is the pool. An average over the nodes would be wrong while
  a sidecar is connected to some of them only.
- An attempt without credit within `assignTimeout` was rejected, failing the task - wrong while a pool scales up from
  few or zero pods. Now it is accepted and reported as RETRY: the queue counts the attempt and hands it out again.

## D26. gRPC keepalive on every long-lived connection

- Without HTTP/2 pings a connection that stays open but carries nothing (a hung network path - seen in k8s) is never
  closed: the node keeps the attempts of such a session "in progress", the sidecar neither reconnects nor delivers
  results. Both ends ping, also without calls.
- Sidecar -> node: `NODE_KEEPALIVE_TIME` 10s, `NODE_KEEPALIVE_TIMEOUT` 5s - a hung node is found in ~15 s. Node:
  pings each connection after `url-manager.grpc-keepalive.time` (30s) of quiet, closes it after `timeout` (10s) -
  the session ends and its attempts are retried; permits pings of sidecars every `permit-time` (5s).
- Sidecar -> handler: `HANDLER_KEEPALIVE_TIME` 30s / `HANDLER_KEEPALIVE_TIMEOUT` 10s; the handler must permit pings
  every 10s without calls (fetcher does) - convention in [handler-sidecar/README.md](../handler-sidecar/README.md).
- grpc-java and grpc-go raise a keepalive time below 10s to 10s; the default of a server allows a client ping per
  5 minutes and closes more eager clients with `too_many_pings`, so both ends are configured together.
- Other connections: between Pekko nodes - the failure detector of the cluster (14b/14c); RabbitMQ - heartbeat of
  the client (60s by default); S3 and the sites - timeouts of the HTTP clients.

## D27. Persistent queue: event-sourced partitions in a Pekko cluster

- One state machine for every engine: `Partition` (core, `queue.partition`) decides events from commands (`submit`,
  `completed`, `tick`, `delivered`) without changing itself; `apply` changes it. The in-memory queue applies events at
  once; `task-grid-queue-pekko` persists them first. Decisions are inside the events - the deadline of an attempt,
  the final result, the end of the pause as absolute time - so a replay gives the same state whatever the settings
  and the clock are then (as in `url-manager-cluster` of the prototype). Effects are carried out after the events are
  applied (in memory - after the lock is released), so a receiver may call the queue again at once.
- `attemptTimeout` (`QueueSettings`, url-manager `attempt-timeout: 15m`, above `max-timeout`): a persistent queue
  outlives its worker; an attempt handed out by a node that is gone would wait forever. Its late result is ignored.
- A partition is a sharded entity `task-grid-partition` with id `k:<partition key>` or `t:<task id>`. The result of an
  attempt finds it by the meta of the task, which now travels back with the result (`AttemptResult.meta`,
  `CompleteRequest.meta = 8` echoed by the sidecar) - on any node, also after the partition moved.
- Effects: `Started` - the attempt to the `ReadyCallback`s of the node of the partition (a `Dispatcher` to its local
  worker); `Ended` with a result - to the result store and the `ResultCallback`s, repeated every `redeliver-after`
  until all succeed, then `Delivered` is persisted (at least once, also after a restart). An attempt in progress is not
  handed out again after recovery: its result may still come; otherwise its deadline retries it.
- Remembered entities with the event-sourced store (survives a restart of the whole cluster, no disk of the nodes),
  snapshots every 100 events (immutable `PartitionSnapshot` through a snapshot adapter, two kept, older events
  deleted). An empty partition stays for `idle-timeout` (2m, above the usual crawl-delay) and then passivates if
  still empty and its pause is over: the URLs of a host often come further apart than the crawl-delay, and each of
  them would otherwise start the entity, replay its journal and stop it again; within the timeout the rest of the
  pause is known from memory, after it the pause is surely over. Correctness does not depend on the value - an entity
  never stops before its pause ends. Before it stops it persists `Rested` (an event of the entity, not of the
  partition) that triggers a snapshot of the empty state: the next recovery of the key reads one small snapshot, and
  the retention deletes the events before it - the journal of a key crawled for months does not grow. Nothing changed
  since the last `Rested` - no new event. A command that reaches an entity after it asked to passivate is not lost: at
  the stop it sends itself a tick through sharding and is started again.
- Serialization of commands, events and snapshots: Jackson CBOR of Pekko; the core types know nothing of Jackson - a
  module with mix-ins (`Blob` subtypes `inline` / `ref`, part of the journal format). The tests serialize every
  message (`serialize-messages = on`) and the journal of the persistence testkit serializes events and snapshots.
- The library does not create the actor system: cluster, journal plugin (url-manager: `pekko-persistence-cassandra`
  with ScyllaDB, 14c) and their settings belong to the application; its own settings - `task-grid.queue.pekko`.
- Known gap until 14c: `complete` is a tell and `NodeService` answers the sidecar before the partition persisted the
  result; a result lost on the way waits for the attempt timeout. With sessions to every node the answer will wait for
  the partition.
