// Package handlertest - a test handler for the tests of the handler adapter and of the sidecar.
//
// The sidecar does not know the types of the handler, so the test handler needs no proto either: the request is
// JSON Call, the response is JSON Reply, '\n' and PadBytes of random bytes.
package handlertest

import (
	"bytes"
	"encoding/json"
	"math/rand/v2"
	"net"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"github.com/deripas/task-grid/handler-sidecar/internal/handler"
)

// Method - the method of the test handler; other methods answer UNIMPLEMENTED.
const Method = "/test.v1.Handler/Call"

// Call - a request to the test handler.
type Call struct {
	Data       string `json:"data,omitempty"`
	PadBytes   int64  `json:"pad_bytes,omitempty"`   // size of the response
	SleepMs    int64  `json:"sleep_ms,omitempty"`    // a long call
	StatusCode int32  `json:"status_code,omitempty"` // google.rpc.Code of the response; 0 - OK
}

// Reply - a response of the test handler.
type Reply struct {
	Data     string            `json:"data"`
	Metadata map[string]string `json:"metadata"` // received x-* metadata
	Pad      []byte            `json:"-"`
}

// Handler - the test handler; counts concurrent calls.
type Handler struct {
	cur, peak, calls atomic.Int32
}

// Serve starts the test handler on 127.0.0.1 and stops it in t.Cleanup.
func Serve(t testing.TB) (*Handler, string) {
	t.Helper()
	h := &Handler{}
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	srv := grpc.NewServer(grpc.ForceServerCodec(handler.RawCodec{}), grpc.UnknownServiceHandler(h.handle))
	go srv.Serve(lis)
	t.Cleanup(srv.Stop)
	return h, lis.Addr().String()
}

// Peak - the largest number of concurrent calls.
func (h *Handler) Peak() int32 {
	return h.peak.Load()
}

// Calls - calls so far.
func (h *Handler) Calls() int32 {
	return h.calls.Load()
}

// Encode - request body for Call.
func Encode(t testing.TB, c Call) []byte {
	t.Helper()
	b, err := json.Marshal(c)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

// Decode parses a response of the test handler.
func Decode(t testing.TB, b []byte) Reply {
	t.Helper()
	head, pad, ok := bytes.Cut(b, []byte{'\n'})
	var r Reply
	if !ok {
		t.Fatalf("reply without header: %d bytes", len(b))
	}
	if err := json.Unmarshal(head, &r); err != nil {
		t.Fatal(err)
	}
	r.Pad = pad
	return r
}

func (h *Handler) handle(_ any, stream grpc.ServerStream) error {
	h.calls.Add(1)
	n := h.cur.Add(1)
	defer h.cur.Add(-1)
	for p := h.peak.Load(); n > p && !h.peak.CompareAndSwap(p, n); p = h.peak.Load() {
	}
	if m, _ := grpc.MethodFromServerStream(stream); m != Method {
		return status.Errorf(codes.Unimplemented, "method %s", m)
	}
	var body []byte
	if err := stream.RecvMsg(&body); err != nil {
		return err
	}
	var req Call
	if err := json.Unmarshal(body, &req); err != nil {
		return status.Error(codes.InvalidArgument, err.Error())
	}
	ctx := stream.Context()
	if req.SleepMs > 0 {
		select {
		case <-time.After(time.Duration(req.SleepMs) * time.Millisecond):
		case <-ctx.Done():
			return status.FromContextError(ctx.Err()).Err()
		}
	}
	if req.StatusCode != 0 {
		return status.Error(codes.Code(req.StatusCode), "test handler: requested status")
	}
	got := map[string]string{}
	md, _ := metadata.FromIncomingContext(ctx)
	for k, v := range md {
		if strings.HasPrefix(k, "x-") && len(v) > 0 {
			got[k] = v[0]
		}
	}
	head, _ := json.Marshal(Reply{Data: req.Data, Metadata: got})
	resp := make([]byte, len(head)+1+int(req.PadBytes))
	copy(resp, head)
	resp[len(head)] = '\n'
	rand.NewChaCha8([32]byte{}).Read(resp[len(head)+1:])
	return stream.SendMsg(&resp)
}
