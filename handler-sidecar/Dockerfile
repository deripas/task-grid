# handler-sidecar image. Build context - this directory (internal/gen is committed, no protoc needed).
FROM golang:1.27 AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY cmd ./cmd
COPY internal ./internal
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/handler-sidecar ./cmd/handler-sidecar

# In k8s the handler is another container of the pod: the sidecar calls it on localhost (HANDLER_TARGET).
FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/handler-sidecar /usr/local/bin/handler-sidecar
ENTRYPOINT ["/usr/local/bin/handler-sidecar"]
