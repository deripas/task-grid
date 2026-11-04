// handler-sidecar connects a handler - any gRPC service with one unary method - to a task-grid node.
// Contracts: the protocol of the node (remote.proto of task-grid-worker-remote) and the handler convention (README.md).
package main

import (
	"os"

	"github.com/deripas/task-grid/handler-sidecar/internal/app"
)

func main() {
	os.Exit(app.Run())
}
