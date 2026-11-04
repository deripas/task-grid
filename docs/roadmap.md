# Roadmap

Moving the core over from the prototype (`crawler-maket/services/dispatch`) in small steps, not 1:1. Each step:
discussion, changes, `make verify`, commit. Status: `[ ]` not started, `[~]` in progress, `[x]` done.
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
8. `[ ]` **Fetcher.** Code generation for `fetcher-api`, gRPC server over Apache HttpClient, tests on WireMock.
9. `[ ]` **Starter.** Spring Boot auto-configuration of a single node.
10. `[ ]` **url-manager: synchronous `POST /fetch`.** End to end url-manager - sidecar - fetcher,
   integration test (`make it`).
11. `[ ]` **url-manager: RabbitMQ with reply_to.** Typed client protocol, limits, deduplication.
12. `[ ]` **Pekko engine.** Clustered url-manager, deployable for load tests.
13. `[ ]` **Later.** gRPC long polling, pauses per key (crawl-delay), blob storage for large results, metrics.
