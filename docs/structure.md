# Structure

Repository layout and module roles. Rules for packages inside a module live in that module's `ArchitectureTest`
(javadoc header) and are not repeated here.

## Layout

```
pom.xml                    root parent: versions, plugins, modules (no Spring Boot)
Makefile                   build entry points: make verify, make test, ...
libs/                      reusable task-grid libraries, no Spring
  task-grid-core/          domain: types, contracts (ports), in-memory implementations; JDK only
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
| `libs/` remote worker library | `Worker` for handlers behind handler-sidecar: gRPC protocol, credits, affinity |
| `libs/` Pekko library | cluster engine: sharded queues and results, cluster-wide dispatch |
| `handler-sidecar` (Go) | process next to a remote handler; location decided with its step |
| `starters/` | auto-configuration of a task-grid node for Spring Boot applications |

## Dependency direction

```
crawler-example/*  ->  starters/*  ->  libs/* (engines, transports)  ->  task-grid-core
```

- `task-grid-core` depends on nothing but the JDK.
- Libraries never depend on Spring; starters never depend on example applications.
- `fetcher` and `url-manager` share only `fetcher-api`; neither depends on the other.

## Conventions

- Maven module directory = artifactId; groupId `io.github.deripas`; base package `io.github.deripas.taskgrid`.
- Every module with more than one package has an `ArchitectureTest` (ArchUnit) run by `make verify`.
