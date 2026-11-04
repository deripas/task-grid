# handler-sidecar

Connects a handler - any gRPC service with one unary method - to a task-grid node. The handler knows nothing
about task-grid: it takes a request and returns a response. Sessions, credit, retries of results and graceful
shutdown are done here once, for handlers in any language.

- Protocol with the node - [remote.proto](../libs/task-grid-worker-remote/src/main/proto/taskgrid/worker/remote/v1/remote.proto)
  (`taskgrid.worker.remote.v1.Node`); the node side is `task-grid-worker-remote`.
- Neither the node nor the sidecar knows the types of the handler: the payload of an attempt is the request body,
  the response body is the result. Types are known to whoever submits the task and to the handler.

## Handler convention

The handler is a gRPC service with a unary method `rpc M(Req) returns (Resp)` of its own types
(`crawler.fetcher.v1.Fetcher/Fetch`, ...). The sidecar calls `HANDLER_METHOD` with a raw codec.

Sidecar -> handler:
- deadline of the call - `HANDLER_TIMEOUT`;
- metadata - `x-task-id`, `x-attempt` and the whole task meta as `x-meta-<key>`. The key is reduced to the characters
  of a gRPC header: lower case, `[0-9a-z_.-]`, others -> `_`, an own `-bin` suffix -> `_bin`. A value outside printable
  ASCII goes as a binary header `x-meta-<key>-bin`.

Handler -> sidecar: the status of the attempt follows the gRPC status, the response body is not parsed.

| Handler status | Attempt status |
|---|---|
| `OK` | `DONE`, result = body (domain errors such as HTTP 404 are content, so `OK` too) |
| `INVALID_ARGUMENT`, `NOT_FOUND`, `FAILED_PRECONDITION`, `PERMISSION_DENIED`, `UNIMPLEMENTED`, `OUT_OF_RANGE`, `ALREADY_EXISTS`, `UNAUTHENTICATED` | `FAILED` |
| response above `MAX_RESPONSE`, or above `INLINE_MAX` with no blob storage | `FAILED` |
| any other (`UNAVAILABLE`, `DEADLINE_EXCEEDED`, `INTERNAL`, `RESOURCE_EXHAUSTED`, `ABORTED`, ...) | `RETRY` |

## What it does

| | |
|---|---|
| Session | one `Connect` stream with the node at `NODE_ADDRESS`: `Hello` (`WORKER_ID`, `KIND`, `CAPACITY`), then credit. Closed by the node with OK - reconnect at once; broken, or the session lived less than a second - after 0.5-1 s |
| Credit | slots belong to the sidecar: unused credit plus attempts in progress <= `CAPACITY`, granted by the demand of the node; surplus credit returned by the node frees the slot. An attempt beyond credit is not executed - `RETRY` |
| Large result | a response above `INLINE_MAX` is put into the S3 bucket `BLOB_S3_BUCKET` (zstd) under `<BLOB_PREFIX><task id>/<random>`, within the slot of the attempt; `Complete` carries `BlobRef` (`s3://<bucket>/<key>`, size and sha256 of the response before compression). Storage fails - `RETRY`. Objects are not deleted here: give the bucket a lifecycle rule (TTL) on the prefix |
| Result | `Complete` repeated every `RETRY_INTERVAL` until OK; `INVALID_ARGUMENT` from the node - given up |
| SIGTERM | drain: no more credit, half-close of the stream; attempts in progress are finished and delivered, the node closes the stream, exit code 0. After `DRAIN_TIMEOUT` the work is abandoned and the exit code is 1: the stop was not clean |

## Deployment and stop order

The sidecar must stop before its handler: while it drains, the handler finishes the attempts in progress, and an
attempt that arrives in the short window before the node sees the half-close still needs a live handler (otherwise
`UNAVAILABLE` -> `RETRY`, a wasted attempt). The handler needs no special command: on SIGTERM it stops taking new
calls, finishes those in progress and exits (ordinary gRPC graceful shutdown; the fetcher does so).

- **Kubernetes 1.29+ - native sidecar.** Declare the handler as an init container with `restartPolicy: Always` and
  handler-sidecar as the main container. The handler starts first (gate it with a `startupProbe`), so the sidecar
  connects to the node only when the handler is ready; on termination the main container (handler-sidecar) gets SIGTERM
  first, and the handler gets it only after the sidecar has drained and exited.

  ```yaml
  spec:
    terminationGracePeriodSeconds: 90       # > DRAIN_TIMEOUT + margin
    initContainers:
      - name: fetcher                       # the handler
        image: crawler/fetcher
        restartPolicy: Always
        startupProbe: { grpc: { port: 50051 } }
    containers:
      - name: handler-sidecar
        image: task-grid/handler-sidecar
        env:
          - { name: KIND, value: fetch }
          - { name: HANDLER_METHOD, value: /crawler.fetcher.v1.Fetcher/Fetch }
          - { name: DRAIN_TIMEOUT, value: 60s }
  ```
- **Older clusters** - both as ordinary containers; the handler gets a `preStop` hook that waits for the sidecar to
  exit (for example a file in a shared `emptyDir`). A plain `sleep` works too but holds the pod for the whole drain.
- **docker-compose** - `depends_on: [handler]` for the sidecar: compose stops services in reverse dependency order;
  `stop_grace_period` above `DRAIN_TIMEOUT`.

Timeouts, e.g. for rendering with 10-30 s per page:

```
HANDLER_TIMEOUT (30-40s)  <  DRAIN_TIMEOUT (60s)  <=  shutdown timeout of the handler
DRAIN_TIMEOUT + margin    <  terminationGracePeriodSeconds (90s)
```

## Settings

| Variable | Default | |
|---|---|---|
| `NODE_ADDRESS` | `localhost:9090` | `host:port` of the node |
| `WORKER_ID` | hostname | stable name of the instance (pod name) |
| `KIND` | - (required) | kind of tasks the handler executes |
| `CAPACITY` | `8` | attempts at once |
| `HANDLER_TARGET` | `localhost:50051` | |
| `HANDLER_METHOD` | - (required) | `/crawler.fetcher.v1.Fetcher/Fetch` |
| `HANDLER_TIMEOUT` | `10m` | limit of one call |
| `MAX_RESPONSE` | `100MiB` | largest response taken from the handler |
| `INLINE_MAX` | `512KiB` | larger responses go to blob storage; at most `4MiB` - the inbound limit of the node |
| `BLOB_S3_ENDPOINT` | - | `host:port` of the S3 storage; not set - no blob storage |
| `BLOB_S3_BUCKET` | `results` | created beforehand, with a lifecycle rule on `BLOB_PREFIX` |
| `BLOB_S3_ACCESS_KEY`, `BLOB_S3_SECRET_KEY` | - | not set - anonymous |
| `BLOB_S3_REGION` | - | not set - asked of the storage |
| `BLOB_S3_SECURE` | `false` | https |
| `BLOB_PREFIX` | - | of the keys, e.g. `results/1h/` |
| `RETRY_INTERVAL` | `1s` | pause between repeats of `Complete` |
| `DRAIN_TIMEOUT` | `60s` | |
| `LOG_LEVEL` | `info` | `debug` - a line per attempt |

## Structure

```
cmd/handler-sidecar/        main: only app.Run
internal/app/               assembly: config -> adapters -> core, stop signals
internal/config/            env -> sidecar.Config, handler.Config: names of variables, defaults, checks
internal/domain/            Attempt, Result, Completion, Status - no protocols, no libraries
internal/sidecar/           core: session, credit, execution, large results, delivery, drain; ports Node, Handler,
                            Blobs (ports.go)
internal/sidecar/credit/    distribution of credit by demand - pure algorithm
internal/node/              node adapter: protocol v1 <-> domain, gRPC codes -> errors of the port
internal/handler/           handler adapter: gRPC with a raw codec, gRPC status -> attempt status
internal/handler/handlertest  test handler (JSON instead of proto) for the tests of handler and sidecar
internal/blob/              blob storage adapter: S3 (minio-go), zstd, keys
internal/gen/               code generated from remote.proto (committed)
```

Package rules - the header of [arch-go.yml](arch-go.yml), checked by `make arch`. Tools (arch-go, protoc plugins) are
in `tools.mod`, so that their dependencies stay out of `go.mod`.

## Build and tests

```bash
make check    # vet, tests with -race, arch-go; part of `make verify` of the repository
make build    # bin/handler-sidecar, static
make gen      # after a change of remote.proto; needs protoc on PATH
make image    # docker image task-grid/handler-sidecar:dev (distroless, static binary)
```

Compatibility with the Java node - `SidecarCompatibilityIT` of `task-grid-worker-remote`: the image is built from this
`Dockerfile` and run by Testcontainers next to SeaweedFS (`make it` in the root); a large result is read back from
the bucket and checked.

Tests: credit (`internal/sidecar/credit`), status mapping and metadata (`internal/handler`), settings
(`internal/config`), format of the objects (`internal/blob`); end to end (`internal/sidecar`: core + real adapters + a fake node over gRPC + the test handler) -
execution within capacity with metadata, failures, repeat of `Complete` until OK and no repeat when rejected,
an attempt beyond credit, drain with an attempt in progress, drain timeout, reconnect, large results (blob,
no storage, storage failure) with a fake storage.

## Not yet

- Sessions with every node of a cluster (DNS), lease (`Extend`), restart detection - with the cluster engine.
- Large payloads (from the node to the handler) in blob storage; `RATE_LIMITED` / `RELEASED` statuses; metrics and
  health probe.
