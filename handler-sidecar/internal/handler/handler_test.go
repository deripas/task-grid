package handler_test

import (
	"context"
	"testing"
	"time"

	"google.golang.org/grpc/codes"

	"github.com/deripas/task-grid/handler-sidecar/internal/domain"
	"github.com/deripas/task-grid/handler-sidecar/internal/handler"
	"github.com/deripas/task-grid/handler-sidecar/internal/handler/handlertest"
)

func client(t *testing.T, maxResponse int) *handler.Client {
	t.Helper()
	_, addr := handlertest.Serve(t)
	c, err := handler.New(handler.Config{Target: addr, Method: handlertest.Method, Timeout: 5 * time.Second, MaxResponse: maxResponse})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { c.Close() })
	return c
}

func TestCallPassesPayloadAndMetadata(t *testing.T) {
	c := client(t, 1<<20)
	r := c.Call(context.Background(), domain.Attempt{
		TaskID:  "t1",
		Number:  3,
		Meta:    map[string]string{"kind": "echo", "affinity-key": "example.com"},
		Payload: handlertest.Encode(t, handlertest.Call{Data: "hello"}),
	})
	if r.Status != domain.Done {
		t.Fatalf("status %v (%s)", r.Status, r.Error)
	}
	resp := handlertest.Decode(t, r.Body)
	if resp.Data != "hello" {
		t.Fatalf("data %q", resp.Data)
	}
	want := map[string]string{"x-task-id": "t1", "x-attempt": "3", "x-meta-kind": "echo", "x-meta-affinity-key": "example.com"}
	for k, v := range want {
		if resp.Metadata[k] != v {
			t.Errorf("metadata %s = %q, want %q", k, resp.Metadata[k], v)
		}
	}
}

// Meta keys are reduced to the characters of a gRPC header, a non-ASCII value goes as a binary header: otherwise
// gRPC would reject the call (INTERNAL -> RETRY), and the task would be retried with no chance to succeed.
func TestMetaSanitized(t *testing.T) {
	c := client(t, 1<<20)
	r := c.Call(context.Background(), domain.Attempt{
		TaskID: "t1",
		Number: 1,
		Meta: map[string]string{
			"Content Type": "text/html",
			"url/path":     "/a/b",
			"raw-bin":      "plain",
			"note":         "привет\n",
		},
		Payload: handlertest.Encode(t, handlertest.Call{}),
	})
	if r.Status != domain.Done {
		t.Fatalf("status %v (%s)", r.Status, r.Error)
	}
	md := handlertest.Decode(t, r.Body).Metadata
	want := map[string]string{
		"x-meta-content_type": "text/html",
		"x-meta-url_path":     "/a/b",
		"x-meta-raw_bin":      "plain",
		"x-meta-note-bin":     "привет\n",
	}
	for k, v := range want {
		if md[k] != v {
			t.Errorf("metadata %s = %q, want %q (got %v)", k, md[k], v, md)
		}
	}
}

func TestStatusMapping(t *testing.T) {
	c := client(t, 1<<20)
	cases := []struct {
		code codes.Code
		want domain.Status
	}{
		{codes.InvalidArgument, domain.Failed},
		{codes.NotFound, domain.Failed},
		{codes.Unimplemented, domain.Failed},
		{codes.Unavailable, domain.Retry},
		{codes.Internal, domain.Retry},
		{codes.DeadlineExceeded, domain.Retry},
		{codes.ResourceExhausted, domain.Retry},
		{codes.Aborted, domain.Retry},
	}
	for _, tc := range cases {
		r := c.Call(context.Background(), domain.Attempt{
			TaskID:  "t",
			Number:  1,
			Payload: handlertest.Encode(t, handlertest.Call{StatusCode: int32(tc.code)}),
		})
		if r.Status != tc.want {
			t.Errorf("%v: status %v, want %v", tc.code, r.Status, tc.want)
		}
		if r.Error == "" || len(r.Body) > 0 {
			t.Errorf("%v: error %q, body %d bytes", tc.code, r.Error, len(r.Body))
		}
	}
}

// A response above MaxResponse: gRPC sets RESOURCE_EXHAUSTED, but a retry will not help.
func TestResponseOverLimitFails(t *testing.T) {
	c := client(t, 1<<20)
	r := c.Call(context.Background(), domain.Attempt{
		TaskID:  "t",
		Number:  1,
		Payload: handlertest.Encode(t, handlertest.Call{PadBytes: 2 << 20}),
	})
	if r.Status != domain.Failed {
		t.Fatalf("status %v (%s), want FAILED", r.Status, r.Error)
	}
}

func TestTimeoutIsRetry(t *testing.T) {
	_, addr := handlertest.Serve(t)
	c, err := handler.New(handler.Config{Target: addr, Method: handlertest.Method, Timeout: 50 * time.Millisecond, MaxResponse: 1 << 20})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	r := c.Call(context.Background(), domain.Attempt{TaskID: "t", Number: 1, Payload: handlertest.Encode(t, handlertest.Call{SleepMs: 1000})})
	if r.Status != domain.Retry {
		t.Fatalf("status %v (%s), want RETRY", r.Status, r.Error)
	}
}
