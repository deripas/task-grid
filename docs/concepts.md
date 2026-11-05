# Concepts

Vocabulary of task-grid. Fields of the types live in code (javadoc), not here: they grow step by step
([D8](decisions.md#d8-minimal-now-extend-when-needed)).

Core does not know the content of tasks: payloads and results are bytes. Types are known only to the ends -
whoever submits a task and the handler that executes it. Everything in between (queue, worker, sidecar) is opaque.

## Flow

```
client ── TaskQueue.submit(Task) ──► queue ── onReady(Attempt) ──► Dispatcher ── Worker.assign(Attempt) ──► handler
                                     (partitions,                     │                              (in-process, or
                                      pauses)  ◄── TaskQueue.complete ◄┴── onComplete(AttemptResult) ◄── behind sidecar)
                                       │
                                       ├── ResultStore.put(TaskResult) ──► client (await)
                                       └── onResult(TaskResult) ──► reply to a client elsewhere
```

Queue and worker do not know each other; `Dispatcher` connects them through their ports
([D24](decisions.md#d24-queue-and-worker-are-decoupled-dispatcher-connects-them)). `assign` only hands the attempt
over (accepted / rejected); its result comes later through `onComplete`. A future would not survive a cluster:
the result may arrive at another node than the one that assigned it.

## Terms

| Term | Meaning |
|---|---|
| **Task** | Unit of work submitted by a client: id (set by the client), payload, meta. |
| **Meta** | Small string properties of a task; see [Meta](#meta). |
| **Kind** | Meta key `kind`. Type of work. Selects the handlers able to execute the task; core does not interpret it. |
| **Partition key** | Meta key `partition-key` (`TaskQueue.PARTITION_KEY`), optional. Tasks with the same key run one at a time, in order - one partition of the queue, with a pause (`DelayPolicy`) after each result (crawl-delay). For crawling - the host: politeness, for every kind. Not set - the task is independent. |
| **Affinity key** | Meta key `affinity-key` (`Worker.AFFINITY_KEY`), optional. Attempts with the same key preferably run on the same handler instance (warm connections, caches); not set - any free instance. An optimization of a pool, independent of the partition key: `fetch` sets both to the host, a renderer that starts a browser per page sets only the partition key. |
| **Attempt** | One execution of a task, numbered from 1. A task has one or more attempts. |
| **Dispatcher** | Connects a queue to a worker: ready attempts to the worker, results back to the queue, a rejected attempt fails its task. One for all implementations, in core. |
| **Attempt timeout** | `QueueSettings.attemptTimeout`: an attempt without a result this long is a RETRY - the limit for a hung attempt or one lost with its node, not a deadline of the work. |
| **Pause** | Time a partition waits after the result of an attempt before its next one (retry or next task), from `DelayPolicy`: crawl-delay of a host. |
| **Pool load** | Per kind, seen by the remote worker: attempts waiting for a free slot, busy slots, capacity - the signals for autoscaling the pools of handlers. |
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
  (`Worker.KIND`, `Worker.AFFINITY_KEY`, `TaskQueue.ATTEMPTS`, `TaskQueue.PARTITION_KEY`). A missing required key is
  a rejection, not a crash.
- Keys of task-grid are short (`kind`, `partition-key`, `affinity-key`); keys of an application carry its prefix
  (`crawler.`, `client.`).
- The whole task meta goes to the handler and into the result. Meta is small: large data belongs to the payload.

## Packages of task-grid-core

| Package | Contents |
|---|---|
| `task` | Task, Attempt, statuses, results, Blob - pure types |
| `queue` | `TaskQueue` port (submit, complete; `onReady` - `ReadyCallback` for attempts ready to run; `onResult` - `ResultCallback` for final results on the node where a task finished), `DelayPolicy`, `QueueSettings` |
| `result` | `ResultStore` port |
| `worker` | `Worker` (assign, `onComplete`), `AttemptCallback`, `Assignment` |
| `dispatch` | `Dispatcher` - connects a queue to a worker |
| `queue.partition` | `Partition` - event-sourced state machine of the tasks of one partition key (commands decide `PartitionEvent`s, `apply` changes the state; time is a parameter), `PartitionSnapshot`; shared by the queue engines |
| `queue.memory` | `MemoryTaskQueue` - single-node engine of partitions: events applied at once, a timer per partition |
| `result.memory` | `MemoryResultStore` - single-node store with TTL |
| `worker.local` | `LocalWorker` - runs in-process handlers on an executor; `Handler`, `RetryableException` |

The remote worker is a separate library, `task-grid-worker-remote`: `RemoteWorker` (engine, pool `Load` by kind),
`Allocator` (credits, affinity), gRPC adapter `NodeService` over `remote.proto`; its rules are in its own
`ArchitectureTest`.

The persistent queue is a separate library, `task-grid-queue-pekko`: `PekkoTaskQueue` over sharded event-sourced
`PartitionEntity`s running the same `Partition`.

Implementations see only ports of other packages. Allowed dependencies are checked by `ArchitectureTest` of the
module.
