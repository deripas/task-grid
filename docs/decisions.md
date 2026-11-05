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
  affinity-wait; without it - any sidecar. No per-kind rule in settings.
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
