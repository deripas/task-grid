# Stand of the crawler

docker-compose of the whole crawler on one machine: three url-managers, two fetchers behind handler-sidecars,
RabbitMQ, SeaweedFS and a fake site. Two profiles, one at a time - the two ways to run url-manager (D28):

| | `memory` | `cluster` |
|---|---|---|
| url-manager | 3 instances, tasks in memory of each | 3 nodes of a Pekko cluster (every node a seed node, SBR keep-majority) |
| Requests | queue `url-manager.fetch.N` per instance, `ack: after-reply` | one queue `url-manager.fetch`, `ack: after-submit` |
| Crawl-delay | per instance: a host always lands on the same queue | across the cluster: a host is one partition |
| A stopped instance | its unacknowledged requests wait in its queue until it is back | its partitions move to the other nodes with the journal |
| Extra | - | ScyllaDB (1 node) and `scylla-init` applying `url-manager/cql/schema.cql` |

Common to both:

- Clients publish `FetchRequest` (`url_manager.proto`) to the exchange `url-manager.fetch` of type
  `x-consistent-hash` with the **host as the routing key**, `reply_to` - their queue. The queues of the profile are
  bound with weight 1 (`rabbitmq/<profile>/definitions.json`).
- `fetcher-N` + `sidecar-N` - a pod: the sidecar shares the network of its fetcher (`network_mode`) and calls it on
  localhost. `NODE_ADDRESS=url-manager:9090` - the network alias of all three url-managers: each sidecar keeps a
  session with each of them (D29), log `session open ... sessions=3`.
- Pages above `INLINE_MAX` (512 KiB) of the sidecar go through the bucket `results` of SeaweedFS.
- `site`: WireMock with the aliases `site-a` ... `site-e`. Any path - `page <host><path>` after 100 ms; `/large` -
  600 KB. Its journal (`/__admin/requests`: Host and time) shows the crawl-delay (1 s) of each host.

## Run

```
make stand PROFILE=memory      # or cluster; builds the jars, then docker compose up --build; Ctrl+C stops
make stand-down                # removes containers and volumes of both profiles
make stand-it                  # StandIT: both profiles under requests, a url-manager stopped in the middle
```

Ports on the host: RabbitMQ `5672`, management UI `15672` (`stand` / `stand`), url-managers `8081`-`8083`
(`/actuator/prometheus` - `task_grid_worker_*` of the pool), site `8090`.

The images copy the build output (`url-manager/target/url-manager.jar`, `fetcher/target/fetcher.jar` + `lib/`):
`make stand` packages them first. The sidecar image is built from `handler-sidecar/Dockerfile`.

## StandIT

`url-manager/src/test/.../StandIT` - Testcontainers `ComposeContainer` over this file, a profile per test (Maven
profile `stand`, not part of `make it`). 41 requests over five hosts; after the first replies:

- `memory` - the instance whose queue has the most messages is restarted;
- `cluster` - `url-manager-cluster-2` is stopped for good.

Then every request must get its page (the large one through blob storage) and two requests of a host must be at
least the crawl-delay apart in the journal of the site.
