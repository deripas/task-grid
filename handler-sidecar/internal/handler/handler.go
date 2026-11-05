// Package handler - adapter to the handler: a unary gRPC call on localhost with a raw codec.
// The handler is any gRPC service with one unary method of its own types: the sidecar does not know them,
// the payload of the attempt is the request body and the response body is the result as is.
// The protocol of the node is unknown here: attempt and result are domain types.
package handler

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"github.com/deripas/task-grid/handler-sidecar/internal/domain"
)

// Config - where and how to call the handler.
type Config struct {
	Target      string        // host:port of the handler, usually localhost
	Method      string        // full method name, e.g. /crawler.fetcher.v1.Fetcher/Fetch
	Timeout     time.Duration // limit of one call
	MaxResponse int           // bytes; a larger response fails the attempt
	// HTTP/2 pings, also between calls: a connection without an answer within KeepaliveTimeout is closed and
	// the calls on it fail (RETRY). gRPC raises KeepaliveTime below 10s to 10s; the handler must permit pings
	// that often.
	KeepaliveTime    time.Duration
	KeepaliveTimeout time.Duration
}

// RawCodec passes message bodies as is: the sidecar does not know the types of the handler.
// Named "proto": on the wire it is ordinary protobuf, the handler parses it with its own types.
type RawCodec struct{}

func (RawCodec) Name() string { return "proto" }

func (RawCodec) Marshal(v any) ([]byte, error) {
	b, ok := v.(*[]byte)
	if !ok {
		return nil, fmt.Errorf("RawCodec: unexpected %T", v)
	}
	return *b, nil
}

func (RawCodec) Unmarshal(data []byte, v any) error {
	b, ok := v.(*[]byte)
	if !ok {
		return fmt.Errorf("RawCodec: unexpected %T", v)
	}
	*b = append((*b)[:0], data...) // gRPC reuses its buffer
	return nil
}

// Client calls the method of the handler.
type Client struct {
	conn    *grpc.ClientConn
	method  string
	timeout time.Duration
}

// New creates a client; the connection is established lazily, on the first call.
func New(cfg Config) (*Client, error) {
	conn, err := grpc.NewClient(cfg.Target,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithKeepaliveParams(keepalive.ClientParameters{
			Time:                cfg.KeepaliveTime,
			Timeout:             cfg.KeepaliveTimeout,
			PermitWithoutStream: true,
		}),
		grpc.WithDefaultCallOptions(
			grpc.ForceCodec(RawCodec{}),
			grpc.MaxCallRecvMsgSize(cfg.MaxResponse),
			grpc.MaxCallSendMsgSize(cfg.MaxResponse),
		))
	if err != nil {
		return nil, err
	}
	return &Client{conn: conn, method: cfg.Method, timeout: cfg.Timeout}, nil
}

func (c *Client) Close() error {
	return c.conn.Close()
}

// Call calls the handler with the payload of the attempt. Metadata: x-task-id, x-attempt and the task meta
// as x-meta-<key> (see metaKey).
func (c *Client) Call(ctx context.Context, a domain.Attempt) domain.Result {
	ctx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()
	md := metadata.Pairs("x-task-id", a.TaskID, "x-attempt", strconv.Itoa(a.Number))
	for k, v := range a.Meta {
		md.Append(metaKey(k, v), v)
	}
	ctx = metadata.NewOutgoingContext(ctx, md)

	req, resp := a.Payload, []byte(nil)
	if err := c.conn.Invoke(ctx, c.method, &req, &resp); err != nil {
		return classify(err)
	}
	return domain.Result{Status: domain.Done, Body: resp}
}

// metaKey - metadata name of a meta key: "x-meta-" + the key reduced to the characters of a gRPC header
// ([0-9a-z_.-]; others -> "_"). A value outside printable ASCII goes as a binary header ("-bin": gRPC passes
// the bytes as is). A key ending with "-bin" gets "_bin": otherwise gRPC would take text for binary.
// gRPC rejects an invalid header (INTERNAL -> RETRY), and the task would be retried with no chance to succeed.
func metaKey(name, value string) string {
	b := []byte(strings.ToLower(name))
	for i, c := range b {
		if !(c >= 'a' && c <= 'z' || c >= '0' && c <= '9' || c == '_' || c == '.' || c == '-') {
			b[i] = '_'
		}
	}
	key := "x-meta-" + string(b)
	if strings.HasSuffix(key, "-bin") {
		key = strings.TrimSuffix(key, "-bin") + "_bin"
	}
	for i := 0; i < len(value); i++ {
		if value[i] < 0x20 || value[i] > 0x7e {
			return key + "-bin"
		}
	}
	return key
}

// classify translates the gRPC status of the handler to a status of the attempt: errors of the request are final,
// everything else is technical and retried.
func classify(err error) domain.Result {
	st, ok := status.FromError(err)
	if !ok {
		st = status.New(codes.Unknown, err.Error())
	}
	if errors.Is(err, context.Canceled) {
		st = status.New(codes.Canceled, err.Error())
	}
	r := domain.Result{Status: domain.Retry, Error: st.Code().String() + ": " + st.Message()}
	switch st.Code() {
	case codes.InvalidArgument, codes.NotFound, codes.FailedPrecondition, codes.PermissionDenied,
		codes.Unimplemented, codes.OutOfRange, codes.AlreadyExists, codes.Unauthenticated:
		r.Status = domain.Failed
	case codes.ResourceExhausted:
		// the gRPC client sets the same code for a response above MaxResponse: a retry will not help
		if strings.Contains(st.Message(), "larger than max") {
			r.Status = domain.Failed
		}
	}
	return r
}
