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
handler-sidecar/           Go process next to a remote handler: session with the node, credit, results, drain
starters/                  Spring Boot starters over libs/ (not necessarily one per library)
crawler-example/           crawler built on task-grid; imports the Spring Boot BOM
  fetcher-api/             fetcher.proto - worker protocol shared by fetcher and url-manager
  fetcher/                 worker: gRPC server over Apache HttpClient, runs behind the sidecar
  url-manager/             Spring Boot application: client protocol (REST, RabbitMQ), queue, dispatcher
docs/                      project documents
.local/                    working notes and scratch files, not in git
```

## Planned modules

Names are tentative until the corresponding roadmap step.

| Module | Role |
|---|---|
| `libs/` Pekko library | cluster engine: sharded queues and results, cluster-wide dispatch |
| `starters/` | auto-configuration of a task-grid node for Spring Boot applications |

## Dependency direction

```
crawler-example/*  ->  starters/*  ->  libs/* (engines, transports)  ->  task-grid-core
```

- `task-grid-core` depends on nothing but the JDK (plus slf4j-api, lombok).
- `task-grid-worker-remote` adds gRPC API and protobuf; the transport (netty, ...) is chosen by the application.
- Libraries never depend on Spring; starters never depend on example applications.
- `fetcher` and `url-manager` share only `fetcher-api`; neither depends on the other.

## Conventions

- Maven module directory = artifactId; groupId `io.github.deripas`; base package `io.github.deripas.taskgrid`.
- Every module with more than one package has an `ArchitectureTest` (ArchUnit) run by `make verify`;
  `handler-sidecar` - `arch-go.yml`, run by `make verify` as well.
- Go module path - `github.com/deripas/task-grid/<dir>`; generated code from shared proto files is committed.
- Integration tests are `*IT` classes run by failsafe in the Maven profile `it` (`make it`, needs Docker);
  `make verify` runs only unit tests and architecture rules.
