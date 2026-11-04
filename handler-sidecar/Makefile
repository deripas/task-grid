BIN := bin/handler-sidecar
IMAGE ?= task-grid/handler-sidecar:dev

.PHONY: build test arch gen check image

## build: static binary in bin/, without debug information
build:
	CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o $(BIN) ./cmd/handler-sidecar

## test: tests with the race detector
test:
	go test -race ./...

## arch: architecture rules (arch-go.yml)
arch:
	go tool -modfile=tools.mod arch-go

## gen: regenerate internal/gen from remote.proto (needs protoc on PATH)
gen:
	go generate ./internal/gen

## image: docker image $(IMAGE)
image:
	docker build -t $(IMAGE) .

## check: vet, tests, architecture rules
check:
	go vet ./...
	$(MAKE) test arch
