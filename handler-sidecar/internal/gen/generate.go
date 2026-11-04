// Package gen holds code generated from the protocol of the node, shared with the Java side:
// libs/task-grid-worker-remote/src/main/proto/taskgrid/worker/remote/v1/remote.proto.
//
// The code is committed. Regenerate after a change of the proto (protoc on PATH; the plugins come from tools.mod):
//
//	make gen
package gen

//go:generate sh -c "protoc -I ../../../libs/task-grid-worker-remote/src/main/proto --plugin=protoc-gen-go=$(go tool -modfile=../../tools.mod -n protoc-gen-go) --plugin=protoc-gen-go-grpc=$(go tool -modfile=../../tools.mod -n protoc-gen-go-grpc) --go_out=../.. --go_opt=module=github.com/deripas/task-grid/handler-sidecar --go-grpc_out=../.. --go-grpc_opt=module=github.com/deripas/task-grid/handler-sidecar taskgrid/worker/remote/v1/remote.proto"
