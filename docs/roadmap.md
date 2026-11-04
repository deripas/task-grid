# Roadmap

Moving the core over from the prototype (`crawler-maket/services/dispatch`) in small steps, not 1:1. Each step:
discussion, changes, `make verify`, commit. Status: `[ ]` not started, `[~]` in progress, `[x]` done.
Agreed decisions - [decisions.md](decisions.md), layout - [structure.md](structure.md).

1. `[x]` **Name and structure.** Coordinates, own root parent, module layout, `Makefile`.
2. `[ ]` **Concepts and vocabulary.** Request / task / attempt, kind, key, worker, node, outcome; core packages;
   in-process worker vs remote worker. Result - `docs/concepts.md`.
3. `[ ]` **task-grid-core: contracts.** Domain types and ports, `ArchitectureTest`.
4. `[ ]` **task-grid-core: in-memory MVP.** Queue per key, results with deduplication, in-process worker;
   one JVM runs the whole flow.
5. `[ ]` **Remote worker.** Dispatcher for workers behind the sidecar: protocol, credits, affinity.
6. `[ ]` **Sidecar (Go).** Move from the prototype; `make check` covers Go too.
7. `[ ]` **Fetcher.** Code generation for `fetcher-api`, gRPC server over Apache HttpClient, tests on WireMock.
8. `[ ]` **Starter.** Spring Boot auto-configuration of a single node.
9. `[ ]` **url-manager: synchronous `POST /fetch`.** End to end url-manager - sidecar - fetcher,
   integration test (`make it`).
10. `[ ]` **url-manager: RabbitMQ with reply_to.** Typed client protocol, limits, deduplication.
11. `[ ]` **Pekko engine.** Clustered url-manager, deployable for load tests.
12. `[ ]` **Later.** gRPC long polling, pauses per key (crawl-delay), blob storage for large results, metrics.
