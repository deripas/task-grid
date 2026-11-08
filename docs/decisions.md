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
  (Superseded by D29: attempts belong to the sidecar process, no lease needed.)
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
  come with the cluster engine; the credit ledger already distributes slots among several sessions. (Superseded by
  D29.)
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
- Typed protocol `crawler.urlmanager.v1` (module `url-manager-api`, folded into url-manager by D28): `FetchRequest{request_id, url, timeout_ms}` ->
  `FetchReply{request_id, page | refusal}`. The page body goes in the reply as is, up to the 100 MiB of the fetcher -
  `max_message_size` of the broker must allow it (RabbitMQ 4: 16 MiB by default); the production url-manager already
  passes such volumes through RabbitMQ.
- The request id (client's `request_id`, else `message_id`) is the task id: a request delivered again while its task
  is queued is not queued twice. *(D32: the request id is the id of the request - `RequestId`; the task id is the hash
  of the payload, so the same page asked for twice is one task.)* A request without reply_to is acknowledged and dropped with a warning; a request
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
  pings each connection after `url-manager.grpc-keepalive.time` (10s, was 30s until D29) of quiet, closes it after
  `timeout` (5s) - the session ends (its attempts - D29); permits pings of sidecars every `permit-time` (5s).
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
  until all succeed, then `Delivered` is persisted (at least once, also after a restart). *(D32: the result is
  delivered before `Ended`, which carries no result; no result store, no `Delivered`.)* An attempt in progress is not
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

## D28. url-manager in a cluster: RabbitMQ after-submit only, results taken after the journal

- `url-manager.queue: memory | cluster`. Cluster: `PekkoTaskQueue` on an actor system of the node (`PekkoCluster`,
  in `app`), journal and snapshots in ScyllaDB through `pekko-persistence-cassandra`; keyspaces and tables -
  `url-manager/cql/schema.cql`, applied beforehand (autocreate of the plugin raced with the first recoveries: the
  snapshot keyspace did not exist yet). Addresses, seed nodes and contact points come from
  `url-manager.cluster.*`, the rest from `application.conf`: Split Brain Resolver `keep-majority`, frame 1 MiB for inline
  results. The node joins after its gRPC server starts and leaves before it stops: partitions hand off while sidecars
  can still deliver results.
- Crawl-delay holds across the cluster: a host is one partition. Requests come over RabbitMQ with `ack: after-submit`
  - the reply is sent by `onResult` on the node of the partition. `after-reply` and `POST /fetch` need the result on the
  node that took the request: refused / off in cluster mode until results can be awaited on any node (to discuss with
  the gRPC client protocol, roadmap 15). Without a consistent-hash exchange `after-reply` of the memory mode works but
  keeps the crawl-delay per instance only. *(D34: with the mailbox on Pekko the result is awaited on any node -
  `after-reply`, `POST /fetch` and the gRPC gateway work in cluster mode.)*
- A result is taken only once the queue has it: `TaskQueue.complete` and `AttemptCallback.complete` return a stage,
  `Sidecars.complete` waits for it, `Complete` answers OK after the partition persisted the result (`UNAVAILABLE`
  otherwise - the sidecar sends it again, also to a restarted node). Closes the gap of D27.
- `url-manager-api` is folded into url-manager: no second consumer of `url_manager.proto` yet.

## D29. Sidecar sessions with every node; attempts belong to the sidecar process

- `NODE_ADDRESS` is a DNS name of all the nodes (k8s headless service, a shared network alias); the sidecar resolves it
  every `NODE_RESOLVE_INTERVAL` (10s) and keeps a session with each address. IPv4 addresses if any, else IPv6: a
  dual-stack name lists each node twice. A node gone from the name keeps its session until it ends, no reconnect.
  One credit ledger for all the sessions: free slots go to the node with the largest unmet demand.
- Pool metrics: the sidecar tells each node its share, `Share{capacity / sessions}`, when the number of its sessions
  changes; `capacity` of a node is the sum of the shares, the sum over the nodes is the pool (D25).
- An attempt in progress belongs to the sidecar process, not to its session: `Hello.instance_id` is random per start
  of the process, `worker_id` (pod name) survives a restart. Addresses are not used to tell processes apart (NAT,
  proxies). On the node:
  - a new session of the same worker supersedes the previous one only as the channel (send to the latest); with the
    same instance id the attempts stay in progress, their results may come through any session or node;
  - another instance id - the process was restarted and lost its attempts: they are reassigned at once;
  - the session broke (no drain) - the attempts wait `rejoin-timeout` (15s) for the process to reconnect, then are
    reassigned; a draining sidecar that breaks the session (drain timeout) is not waited for;
  - reassigned - back to wait for credit of other sidecars of the kind with the same attempt number: the queue does not
    see it and `maxAttempts` is not spent. A late result of the old sidecar is harmless - the queue takes the first one
    for the attempt. Endless reassignment is bounded by the attempt timeout of the queue.
- Fast detection makes the wait short: a killed process closes its sockets (RST) at once; a silent loss (a machine
  gone, a hung path) is found by keepalive, now 10s/5s on both ends (~15 s).
- A result goes to the node the attempt came from while that node is known (a node with a queue of its own - memory
  mode - would ignore it elsewhere); when it is gone from the name, the tries go round the other nodes: in a cluster
  any node finds the partition by the meta. `RemoteWorker.complete` frees the attempt on the node that has it, others
  just pass the result to the queue.
- Node stop: sessions are closed, attempts in progress are left to their sidecars (their results go to another node;
  the attempt timeout of the queue is the backstop); attempts waiting for credit, and those assigned after the stop,
  are reported as `RETRY` instead of rejected - a reject failed the task.

## D30. docker-compose stand with both ways to run url-manager

- `crawler-example/stand/compose.yaml`, profiles `memory` and `cluster`, one at a time (`make stand PROFILE=...`).
  Clients are the same for both: `FetchRequest` to the exchange `url-manager.fetch` of type `x-consistent-hash`,
  routing key = host, as in production. The profile decides what is bound behind it (`rabbitmq/<profile>/
  definitions.json`): `memory` - a queue per instance (crawl-delay per instance holds because a host always lands on
  the same one), `cluster` - one queue for all the nodes. The exchange and queues are infrastructure: url-manager
  declares only its own queue, as before.
- A fetcher and its sidecar are a pod: `network_mode: service:fetcher-N`, the handler on localhost. The url-managers
  share a network alias - the DNS name of all the nodes the sidecars connect to (D29).
- Images copy the build output (Spring Boot jar of url-manager; `fetcher.jar` with `lib/`), no build inside Docker:
  `make stand` packages first, `StandIT` runs after the package phase.
- `StandIT` (Testcontainers `ComposeContainer`, Maven profile `stand`, `make stand-it`; minutes, so not in
  `make it`): both profiles under 41 requests over five hosts; `memory` restarts its busiest instance, `cluster`
  loses a node for good; every page must come and a host is fetched no more often than the crawl-delay (journal of
  the WireMock site). It replaces the in-JVM test of two memory instances planned before: one DNS name cannot point to
  two ports of the test JVM, so a sidecar could not reach both.

## D31. Client gateway: results wait in the mailbox of a consumer

- The entry points of clients (transports) are gateways: `task-grid-gateway-<transport>` libraries over `TaskQueue`.
  A gateway does not know the application: the task (payload, meta) and the reply are made by a codec of the
  application, as the body of a RabbitMQ message is `FetchRequest` while `reply_to` belongs to the transport.
- A transport without a reply address (gRPC, REST) needs the result on another node than the one that took the task.
  It waits in a mailbox (`Mailbox`, core): a box per consumer - client (an integration) + consumer (its instance,
  stable across restarts, e.g. the ordinal of a pod); any node reaches any box (in memory - only the node itself,
  so a gRPC gateway over memory boxes serves one node; the target is the cluster). Not a relay to the node of the
  connection: a reconnect to another node, a restart of the client would lose results.
- Limit per box: tasks submitted and not yet acknowledged. A fuse against a client that submits without taking its
  results, not load balancing - so a plain API, no credits (unlike the workers, which scale under the load).
  A full box refuses the submit.
- Idempotent by the request id (`RequestId`: client, consumer, id - D32): a client retries a submit with the same id.
  `reserve` tells the gateway what to do - a new request (submit; release the place if the submit fails), a pending
  one (submit again, the queue keeps one request per id), done (the result waits in the box - do not submit again).
- Delivery at least once: a subscription streams the results not yet acknowledged in the order they were put, then
  the new ones, within the demand of the stream; an ack removes a result and frees its place. Not acknowledged -
  sent again with the next subscription; a result put again after its ack (the result callback is retried) comes
  again. A newer subscription of a box ends the older one.
- One ttl for everything in a box: a reservation whose task never reached the queue (a node failed between reserve
  and submit, the client did not retry), a result nobody takes, an abandoned box.

## D32. One task for the same payload; results leave the queue before its end

- The queue keeps the requests, the mailbox keeps the results: one copy of each. Before, the journal of a partition
  had the payload in `Submitted` and again in every `Started`, the result in `Ended` (kept until delivered,
  `Delivered` after it) and in the snapshots; a box would have been a second copy of the result.
- A task is the work, a request is who asked. `TaskQueue.submit(Task, Request)`: `Request` = `RequestId` (client,
  consumer, the id of the client - the key of idempotency and the address of the result) + tags of the requester (e.g.
  the correlation id of AMQP). The id of a task is the hash of its payload (`Task.of`); the meta is not part of it -
  the meta of the first request stays (it is of the work: kind, keys). The same payload while the task is in its
  partition joins it (`Joined`): done once, the result goes to every request. The same request again changes nothing;
  a request id with another task is an error. After the task finished the same payload is a new task: no cache of
  results. A client that needs its own execution makes the payload differ.
- Events: `Submitted(task, request)`, `Joined(taskId, request)`, `Started(taskId, attempt, deadline)` - no payload,
  `Ended(taskId, attempt, finished, resumeAt)` - no result. A command of `Partition` returns a `Step`: the events and
  the final result with its requests. The Pekko entity delivers that result to the `ResultCallback`s first - other
  commands stashed, at most `delivery-timeout` - and persists the events only then. A failed delivery is decided
  again: a result of a worker fails its `Complete` (the sidecar repeats it), a result of a deadline is decided again
  by a tick after `redeliver-after`. A receiver that is down holds its partitions: backpressure instead of results
  piling up in the journal.
- `ResultStore` is gone. Waiting on the node that took the request (`POST /fetch`, AMQP `after-reply`) goes through
  the mailbox: box (`rest` | `amqp`, `url-manager.node-id`), a new request id per wait; the node subscribes to its
  boxes, hands a reply to the waiting request and acknowledges it at once. A wait reserves its place first:
  `url-manager.box-limit` requests of a client in work per node - a business limit, whatever the transports; a
  request that timed out keeps its place until its result is taken. A full box - `FetchOutcome.Busy`: 429 for REST,
  `CODE_BUSY` for AMQP. AMQP `after-submit`: `RequestId(amqp-reply-to, reply_to, request_id)`, the correlation
  id in the tags; the reply is published by its `ResultCallback`.

## D33. gRPC gateway: unary submit, a stream of results, a stream of acks

- `taskgrid.gateway.v1.TaskGateway` (`libs/task-grid-gateway-grpc`): `Submit` (unary) - `OK` once the request is in
  the queue, `RESOURCE_EXHAUSTED` - the box is full, `INVALID_ARGUMENT` - refused by the application or the id is in
  the queue with another payload, `UNAVAILABLE` - retry with the same id; `Subscribe` - a server stream of the results
  of a box not acknowledged, then the new ones, a newer subscription ends the older one with `ABORTED`; `Ack` -
  a client stream of request ids. No credits: the limit of a box is a fuse, a client that reads fast submits fast;
  flow control of the stream is that of gRPC (the next result is taken from the box when the stream is ready).
- The gateway knows no application: `TaskCodec` of the application makes the task of a payload (`Task.of` - the same
  payload of several requests is one task) and the reply of a result. A reply may block (a large page from blob
  storage): made on an executor of the gateway, one at a time per stream; an exception ends the stream, the result
  comes again with the next subscription.
- Which results go to the mailbox is marked on the request (tag `mailbox`, `MailboxSink.request`), not known to the
  node of the request: the result finishes on the node of its partition, `MailboxSink` (registered on every node with
  the queue) puts it into the box. The same for the waits of url-manager (`rest`, `amqp` boxes of its node); AMQP
  `after-submit` requests are not marked - their callback publishes the reply.
- url-manager: `FetchTaskCodec` - payloads are `FetchRequest` / `FetchReply` of `crawler.urlmanager.v1`, the same as
  over RabbitMQ (`request_id` of the reply - the id of the gateway; `timeout_ms` is not used, the result waits in the
  box); a page that cannot be read is `CODE_FAILED`. The gateway is on the gRPC server of the node (port 9090, next
  to the sidecars), the boxes have the `box-limit` of url-manager. Only in memory mode for now: in a cluster the result
  would reach the mailbox of another node - the mailbox on Pekko comes first. A large page is in the message: a
  client sets its max inbound message size (gRPC: 4 MiB by default) up to the pages it expects.

## D34. Mailbox on Pekko: a box is a sharded event-sourced entity

- `task-grid-mailbox-pekko`: `PekkoMailbox` over `BoxEntity` (`task-grid-box`, entity id - `client/consumer`).
  Events `Reserved`, `Released`, `Stored` (the result), `Acked`, `Expired`; the expiry is decided in the event. A box
  with requests is a remembered entity and stays in memory: a timer at the earliest expiry persists `Expired`, also
  after a restart of the cluster. An empty box without a subscriber passivates after `idle-timeout`; the event that
  empties a box takes a snapshot of the empty state (events before it deleted): the journal of a long-lived box does
  not grow.
- The subscription is not persisted. A subscriber is an actor on its own node (`BoxSubscriber`, a `Flow.Subscriber`
  bridge): it passes the demand to the box and the results to the subscriber. The box watches it (a node gone - the
  subscription ends) and sends within its demand what this subscriber has not got. The box moved or restarted: it
  does not know the subscriber - the next heartbeat (`heartbeat`, 5s) or demand gets `Unknown`, the subscriber
  subscribes again with the demand not served, the box sends again what is not acknowledged (at least once). Another
  subscriber has the box - the older one gets `Superseded`. On a stop of the box for a rebalance the subscriber is
  told at once; not during the shutdown of the node (it would start the box again).
- `task-grid-pekko-core`: the Jackson module for the types of the core (`Blob`), shared by the Pekko libraries
  through reference.conf.
- url-manager: `PekkoMailbox` in cluster mode, `MemoryMailbox` in memory mode; `after-reply` is no longer refused in
  the cluster, `POST /fetch` and the gRPC gateway are on in both modes. `StandIT` (cluster): a gateway client submits
  to one node and takes its results from another while a third node leaves.

## D35. Gateways in core, transports in libraries; AMQP on amqp-client

- `gateway` of task-grid-core: `Gateway` (reserve in the box, submit marked for the mailbox, `call` - wait on this
  node through the box of the client and the node), `TaskCodec` of the application (task of a payload, reply of a
  result, refusal: `INVALID`, `FAILED`, `TIMEOUT`, `BUSY`), `Call`, `Submission`. Core shows the whole way from a
  client to a worker; the transports are libraries over it: `task-grid-gateway-grpc`, `task-grid-gateway-amqp`.
- `task-grid-gateway-amqp` on `com.rabbitmq:amqp-client`, no Spring (libraries never depend on it): a consumer with
  manual acknowledgement and prefetch, replies through the default exchange with publisher confirms, automatic
  recovery; the queue is declared durable if absent. Both ways of D22 / D28: `after-reply` - `Gateway.call` with the
  box `amqp` of the node, the request acknowledged after the confirmed reply; `after-submit` - `RequestId(
  amqp-reply-to, reply_to, id)` with the correlation id in the tags, the reply published by its `ResultCallback`.
- The id of a request is the hash of its message: a message delivered again is the same request, nothing else to
  agree on with the clients. `FetchReply.request_id` is the id of the request on its transport (the correlation id
  of the message, else the hash; the id of a gRPC request); `FetchRequest.request_id` and `timeout_ms` are not read -
  an after-reply request waits `url-manager.default-timeout`. `POST /fetch` waits through the same `Gateway.call`, its
  id - the hash of its task.
- url-manager: `FetchTaskCodec` (`protocol`) for both transports, `RabbitRequests` - the Spring lifecycle of
  `AmqpGateway` (stops first), its connection - a clone of the connection factory of Spring Boot
  (`spring.rabbitmq.*`); prefetch - `url-manager.rabbit.prefetch`. Spring AMQP is left only for that factory.
