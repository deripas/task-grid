# Concepts

Vocabulary of task-grid. Fields of the types live in code (javadoc), not here: they grow step by step
([D8](decisions.md#d8-minimal-now-extend-when-needed)).

Core does not know the content of tasks: payloads and results are bytes. Types are known only to the ends -
whoever submits a task and the handler that executes it. Everything in between (queue, worker, sidecar) is opaque.

## Flow

```
client ── TaskQueue.submit(Task) ──► queue (per affinity key) ── Worker.assign(Attempt) ──► handler
                                       ▲                                                    (in-process, or remote
                                       └──────── AttemptCallback.complete(AttemptResult) ◄── behind handler-sidecar)
                                       │
                                       └── ResultStore.put(TaskResult) ──► client (await)
```

`assign` only hands the attempt over (accepted / rejected); its result comes later through `AttemptCallback`.
A future would not survive a cluster: the result may arrive at another node than the one that assigned it.

## Terms

| Term | Meaning |
|---|---|
| **Task** | Unit of work submitted by a client: id (set by the client), payload, meta. |
| **Meta** | Small string properties of a task; see [Meta](#meta). |
| **Kind** | Meta key `kind`. Type of work. Selects the handlers able to execute the task; core does not interpret it. |
| **Partition key** | Meta key `partition-key` (`TaskQueue.PARTITION_KEY`), optional. Tasks with the same key run one at a time, in order - one partition of the queue; `DelayedWorker` adds a pause between them (crawl-delay). For crawling - the host: politeness, for every kind. Not set - the task is independent. |
| **Affinity key** | Meta key `affinity-key` (`Worker.AFFINITY_KEY`), optional. Attempts with the same key preferably run on the same handler instance (warm connections, caches); not set - any free instance. An optimization of a pool, independent of the partition key: `fetch` sets both to the host, a renderer that starts a browser per page sets only the partition key. |
| **Attempt** | One execution of a task, numbered from 1. A task has one or more attempts. |
| **Worker** | Core port: executes attempts. In-process implementation calls handlers in the JVM; remote one sends attempts to sidecars. The prototype called it `Dispatch`. |
| **Handler** | The code doing the work for a kind: a Java interface in the JVM, or a process behind a sidecar exposing one unary gRPC method (for example `fetcher`). |
| **handler-sidecar** | Go process deployed next to a remote handler: session with the node, credits, completion retries, drain, large results to S3 (later: sessions with every node of a cluster, lease extension). Convention for handlers - [handler-sidecar/README.md](../handler-sidecar/README.md). |
| **Node** | One JVM process of task-grid: queues, results, worker. |
| **Attempt status** | How an attempt ended. `DONE` - there is a result; domain errors (HTTP 404) are `DONE` too, they are content. `FAILED` - final, no retry. `RETRY` - technical failure, the attempt counts. Later statuses come with their mechanisms: `RATE_LIMITED` with pauses per key, `RELEASED` with drain. |
| **Task status** | Final status for the client: `DONE` or `FAILED` (final failure or attempts exhausted). |
| **Task result** | Final result of a task: status, body or error, meta of the task plus keys added on the way (`attempts`). |
| **Blob** | Bytes of a payload or result: inline (in heap) or a reference to blob storage; size and SHA-256 hash of the content. The domain type does no I/O; reading a reference is the job of an adapter. |

## Meta

- A component that uses a property declares its key as a constant and reads it in one place
  (`Worker.KIND`, `Worker.AFFINITY_KEY`, `TaskQueue.ATTEMPTS`, `TaskQueue.PARTITION_KEY`); a decorator may read the key
  of the component it serves (`DelayedWorker` - the partition key). A missing required key is a rejection, not a crash.
- Keys of task-grid are short (`kind`, `partition-key`, `affinity-key`); keys of an application carry its prefix
  (`crawler.`, `client.`).
- The whole task meta goes to the handler and into the result. Meta is small: large data belongs to the payload.

## Packages of task-grid-core

| Package | Contents |
|---|---|
| `task` | Task, Attempt, statuses, results, Blob - pure types |
| `queue` | `TaskQueue` port (submit; `onResult` - `ResultCallback` for final results on the node where a task finished), `QueueSettings` |
| `result` | `ResultStore` port |
| `worker` | `Worker`, `AttemptCallback` |
| `queue.memory` | `MemoryTaskQueue` - single-node queue; `Partition` - pure state machine of the tasks of one affinity key |
| `result.memory` | `MemoryResultStore` - single-node store with TTL |
| `worker.local` | `LocalWorker` - runs in-process handlers on an executor; `Handler`, `RetryableException` |

The remote worker is a separate library, `task-grid-worker-remote`: `RemoteWorker` (engine), `Allocator` (credits,
affinity), gRPC adapter `NodeService` over `remote.proto`; its rules are in its own `ArchitectureTest`.

Implementations see only ports of other packages. `Partition` moves to a shared place when a second engine
(Pekko) needs it. Allowed dependencies are checked by `ArchitectureTest` of the module.
