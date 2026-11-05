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
12. `[x]` **Crawl-delay.** `task-grid-worker-delayed`: a worker decorator holds the next attempt of an affinity key
   back until a delay after the result of the previous one (finish to start); url-manager - 1 s per host. Delay from
   robots.txt, cooldown after 429 - through `DelayPolicy`, later. Requirements -
   `crawler-maket/domain-queue-requirements.md`.
13. `[x]` **url-manager: RabbitMQ with reply_to.** Typed client protocol (`url-manager-api`), Spring AMQP; two ways
   of handling by `ack`: `after-submit` (target: acknowledged once queued, the reply from the result callback of the
   queue, `TaskQueue.onResult`) and `after-reply` (transitional, for the in-memory queue: acknowledged after the
   reply is confirmed; requests of a host sharded to one instance by a consistent-hash exchange).
14. `[ ]` **Pekko engine.** Clustered url-manager (queue partitions by host across nodes), deployable for load tests.
15. `[ ]` **Later.** Spring Boot starter, gRPC long polling, metrics.
