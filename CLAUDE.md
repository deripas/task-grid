# task-grid

Task management core with a distributed worker pool: queue -> dispatcher -> worker -> result. Crawling is one
application of it (`crawler-example/`). Started from the prototype `crawler-maket/services/dispatch`; moved over
in small steps, not 1:1.

- Plan and status - [docs/roadmap.md](docs/roadmap.md)
- Agreed decisions - [docs/decisions.md](docs/decisions.md)
- Layout and module roles - [docs/structure.md](docs/structure.md)

## Rules

- Everything in git is in English (code, comments, docs, this file). Discussion with the user is in Russian.
- Details go to `docs/`, not here; keep this file short.
- `.local/` - working notes, drafts, scratch files (not in git); use it instead of `/tmp`.
- Production quality, clean architecture. Package rules - `ArchitectureTest` of each module, run by the build;
  the layer scheme lives only in its javadoc.
- Work by roadmap steps: step -> discussion -> changes -> `make verify` -> commit when asked.
- When moving code from the prototype, review names and decisions instead of copying.

## Build

- `make verify` - build, tests, architecture rules. Maven only via `./mvnw` (no `mvn` in PATH).
