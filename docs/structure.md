# Structure

Repository layout and module roles. Rules for packages inside a module live in that module's `ArchitectureTest`
(javadoc header) and are not repeated here.

## Layout

```
pom.xml                    root parent: versions, plugins, modules (no Spring Boot)
Makefile                   build entry points: make verify, make test, ...
libs/                      reusable task-grid libraries, no Spring
  task-grid-core/          domain: types, contracts (ports), in-memory implementations; JDK only
  task-grid-worker-remote/ Worker for handlers behind handler-sidecar: remote.proto (shared with Go), credits, gRPC
  task-grid-pekko-core/    what the Pekko libraries share: Jackson for the types of the core
  task-grid-queue-pekko/   persistent queue of a Pekko cluster: a partition is a sharded event-sourced entity
  task-grid-mailbox-pekko/ mailboxes of a Pekko cluster: a box is a sharded event-sourced entity, subscribers on any node
  task-grid-gateway-grpc/  clients over gRPC: gateway.proto, a stream of the results of a box, ack
  task-grid-gateway-amqp/  clients over RabbitMQ (amqp-client): request messages, replies to reply_to, ack after-reply
                           or after-submit; the gateways run Gateway of the core with the codec of the application
handler-sidecar/           Go process next to a remote handler: sessions with every node, credit, results (large ones to S3), drain
starters/                  Spring Boot starters over libs/ (not necessarily one per library)
crawler-example/           crawler built on task-grid; imports the Spring Boot BOM
  fetcher-api/             fetcher.proto - handler protocol shared by fetcher and url-manager
  fetcher/                 handler: plain Java gRPC server over Apache HttpClient, runs behind handler-sidecar
  url-manager/             Spring Boot application: RabbitMQ with reply_to (url_manager.proto) and POST /fetch on a
                           task-grid node - in memory, or in a Pekko cluster with the journal in ScyllaDB -, crawl-delay,
                           remote worker of the fetch kind, large pages read from S3, pool metrics on /actuator/prometheus
  stand/                   docker-compose stand: profiles memory and cluster, RabbitMQ, SeaweedFS, ScyllaDB, a fake site
docs/                      project documents
.local/                    working notes and scratch files, not in git
```

## Planned modules

Names are tentative until the corresponding roadmap step.

| Module | Role |
|---|---|
| `starters/` | auto-configuration of a task-grid node for Spring Boot applications |

## Dependency direction

```
crawler-example/*  ->  starters/* (later)  ->  libs/* (engines, transports)  ->  task-grid-core
```

Until there are starters, `url-manager` wires the libraries itself (its `UrlManagerConfiguration`).

- `task-grid-core` depends on nothing but the JDK (plus slf4j-api, lombok).
- `task-grid-worker-remote` adds gRPC API and protobuf; the transport (netty, ...) is chosen by the application.
- Libraries never depend on Spring; starters never depend on example applications.
- `fetcher` and `url-manager` share only `fetcher-api`; neither depends on the other (`UrlManagerIT` runs the fetcher
  in its test JVM: a test dependency only).
- `crawler-example` imports the gRPC and JUnit BOMs before the Spring Boot BOM: versions of task-grid win (generated
  stubs need the gRPC runtime of their plugin).

## Conventions

- Maven module directory = artifactId; groupId `io.github.deripas`; base package `io.github.deripas.taskgrid`.
- Every module with more than one package has an `ArchitectureTest` (ArchUnit) run by `make verify`;
  `handler-sidecar` - `arch-go.yml`, run by `make verify` as well.
- Go module path - `github.com/deripas/task-grid/<dir>`; generated code from shared proto files is committed.
- Integration tests are `*IT` classes run by failsafe in the Maven profile `it` (`make it`, needs Docker);
  `make verify` runs only unit tests and architecture rules. `StandIT` (the docker-compose stand) - only in the profile
  `stand` (`make stand-it`): minutes per profile.
