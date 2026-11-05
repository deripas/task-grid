// End-to-end tests of the sidecar: the core with the real adapters node and handler against fake nodes over gRPC
// and the test handler; addresses of the nodes come from a fake resolver. External package: the node adapter imports
// sidecar itself.
package sidecar_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/deripas/task-grid/handler-sidecar/internal/domain"
	remotev1 "github.com/deripas/task-grid/handler-sidecar/internal/gen/remote/v1"
	"github.com/deripas/task-grid/handler-sidecar/internal/handler"
	"github.com/deripas/task-grid/handler-sidecar/internal/handler/handlertest"
	"github.com/deripas/task-grid/handler-sidecar/internal/node"
	"github.com/deripas/task-grid/handler-sidecar/internal/sidecar"
)

const (
	wait      = 5 * time.Second
	inlineMax = 1 << 10
)

// fakeNode behaves like the real node: keeps the attempts of the session in a queue, tells its demand and sends
// attempts only within credit; after the half-close of the sidecar sends nothing and closes the stream once
// the attempts sent are completed. rogue - sends attempts without credit too.
type fakeNode struct {
	remotev1.UnimplementedNodeServer
	addr string
	srv  *grpc.Server

	hellos         chan *remotev1.Hello
	shares         chan float64
	completes      chan *remotev1.CompleteRequest
	halfClosed     chan struct{}
	failCompletes  atomic.Int32 // that many first Complete calls answer UNAVAILABLE
	rejectComplete atomic.Bool  // Complete answers INVALID_ARGUMENT
	completeCalls  atomic.Int32
	closeAtOnce    atomic.Bool // the session is closed right after Hello
	rogue          atomic.Bool

	mu      sync.Mutex
	sess    *fakeSession
	pending []*remotev1.Attempt // attempts pushed before a session is open
}

type fakeSession struct {
	stream     grpc.BidiStreamingServer[remotev1.SidecarMessage, remotev1.NodeMessage]
	queue      []*remotev1.Attempt
	inProgress map[string]bool
	sent       int64
	limit      int64
	version    int64
	target     int64
	draining   bool
}

func startNode(t *testing.T) *fakeNode {
	t.Helper()
	n := &fakeNode{
		hellos:     make(chan *remotev1.Hello, 100),
		shares:     make(chan float64, 100),
		completes:  make(chan *remotev1.CompleteRequest, 100),
		halfClosed: make(chan struct{}, 10),
	}
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	n.addr = lis.Addr().String()
	n.srv = grpc.NewServer()
	remotev1.RegisterNodeServer(n.srv, n)
	go n.srv.Serve(lis)
	t.Cleanup(n.srv.Stop)
	return n
}

// hello - the next Hello the node got.
func (n *fakeNode) hello(t *testing.T) *remotev1.Hello {
	t.Helper()
	select {
	case h := <-n.hellos:
		return h
	case <-time.After(wait):
		t.Fatal("no hello")
		return nil
	}
}

// awaitShare waits until the node is told the share.
func (n *fakeNode) awaitShare(t *testing.T, want float64) {
	t.Helper()
	deadline := time.After(wait)
	for {
		select {
		case got := <-n.shares:
			if got == want {
				return
			}
		case <-deadline:
			t.Fatalf("no share %v", want)
		}
	}
}

// push queues attempts for the sidecar.
func (n *fakeNode) push(attempts ...*remotev1.Attempt) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.sess == nil {
		n.pending = append(n.pending, attempts...)
		return
	}
	n.sess.queue = append(n.sess.queue, attempts...)
	n.pump()
}

func (n *fakeNode) Connect(stream grpc.BidiStreamingServer[remotev1.SidecarMessage, remotev1.NodeMessage]) error {
	first, err := stream.Recv()
	if err != nil {
		return err
	}
	n.hellos <- first.GetHello()
	if n.closeAtOnce.Load() {
		return nil
	}
	n.mu.Lock()
	n.sess = &fakeSession{stream: stream, queue: n.pending, inProgress: map[string]bool{}, target: -1}
	n.pending = nil
	n.pump()
	n.mu.Unlock()
	for {
		msg, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return err
		}
		if sh := msg.GetShare(); sh != nil {
			n.shares <- sh.Capacity
		}
		if c := msg.GetCredit(); c != nil {
			n.mu.Lock()
			if c.Version > n.sess.version {
				n.sess.limit, n.sess.version = c.Limit, c.Version
			}
			n.pump()
			n.mu.Unlock()
		}
	}
	// drain: no more attempts; close the stream when the attempts sent are completed
	n.halfClosed <- struct{}{}
	n.mu.Lock()
	n.sess.draining = true
	n.mu.Unlock()
	for {
		n.mu.Lock()
		idle := len(n.sess.inProgress) == 0
		n.mu.Unlock()
		if idle {
			return nil
		}
		select {
		case <-time.After(5 * time.Millisecond):
		case <-stream.Context().Done():
			return stream.Context().Err()
		}
	}
}

func (n *fakeNode) Complete(_ context.Context, req *remotev1.CompleteRequest) (*remotev1.CompleteResponse, error) {
	n.completeCalls.Add(1)
	if n.rejectComplete.Load() {
		return nil, status.Error(codes.InvalidArgument, "rejected by the test")
	}
	if n.failCompletes.Add(-1) >= 0 {
		return nil, status.Error(codes.Unavailable, "node is down")
	}
	n.mu.Lock()
	if n.sess != nil {
		delete(n.sess.inProgress, req.TaskId)
	}
	n.mu.Unlock()
	n.completes <- req
	return &remotev1.CompleteResponse{}, nil
}

// pump sends demand when it changed and attempts within credit; under mu.
func (n *fakeNode) pump() {
	s := n.sess
	if s == nil || s.draining {
		return
	}
	for len(s.queue) > 0 && (s.sent < s.limit || n.rogue.Load()) {
		a := s.queue[0]
		s.queue = s.queue[1:]
		s.sent++
		s.inProgress[a.TaskId] = true
		_ = s.stream.Send(&remotev1.NodeMessage{Msg: &remotev1.NodeMessage_Attempt{Attempt: a}})
	}
	if target := s.sent + int64(len(s.queue)); target != s.target {
		s.target = target
		_ = s.stream.Send(&remotev1.NodeMessage{Msg: &remotev1.NodeMessage_Demand{Demand: &remotev1.Demand{
			Target: target, Version: s.version,
		}}})
	}
}

func (n *fakeNode) result(t *testing.T) *remotev1.CompleteRequest {
	t.Helper()
	select {
	case r := <-n.completes:
		return r
	case <-time.After(wait):
		t.Fatal("no Complete")
		return nil
	}
}

// fakeBlobs - blob storage in memory; fail - Put answers an error.
type fakeBlobs struct {
	mu   sync.Mutex
	puts map[string][]byte // location -> answer
	fail atomic.Bool
}

func (b *fakeBlobs) Put(_ context.Context, taskID string, data []byte) (domain.BlobRef, error) {
	if b.fail.Load() {
		return domain.BlobRef{}, errors.New("storage is down")
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	location := fmt.Sprint("mem://", taskID, "/", len(b.puts))
	b.puts[location] = data
	return domain.BlobRef{Location: location, Size: int64(len(data)), Sha256: "sha"}, nil
}

// fakeNodes - the addresses of the nodes as the test sets them; clients are real node adapters.
type fakeNodes struct {
	mu    sync.Mutex
	addrs []string
}

func (r *fakeNodes) Resolve(context.Context) ([]string, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return slices.Clone(r.addrs), nil
}

func (r *fakeNodes) Dial(address string) (sidecar.Node, error) {
	return node.New(node.Config{Address: address, KeepaliveTime: time.Minute, KeepaliveTimeout: 10 * time.Second})
}

// set - the nodes behind the address from now on.
func (r *fakeNodes) set(nodes ...*fakeNode) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.addrs = nil
	for _, n := range nodes {
		r.addrs = append(r.addrs, n.addr)
	}
}

type harness struct {
	nodes   *fakeNodes
	handler *handlertest.Handler
	done    chan error
	cancel  context.CancelFunc
}

func start(t *testing.T, n *fakeNode, capacity int, drainTimeout time.Duration) *harness {
	t.Helper()
	return startWith(t, []*fakeNode{n}, capacity, drainTimeout, nil)
}

// startWith - with the nodes and blob storage; blobs nil - none.
func startWith(
	t *testing.T,
	nodes []*fakeNode,
	capacity int,
	drainTimeout time.Duration,
	blobs *fakeBlobs,
) *harness {
	t.Helper()
	h, addr := handlertest.Serve(t)
	hc, err := handler.New(handler.Config{Target: addr, Method: handlertest.Method, Timeout: 10 * time.Second, MaxResponse: 1 << 20})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { hc.Close() })
	resolver := &fakeNodes{}
	resolver.set(nodes...)
	s := sidecar.New(sidecar.Config{
		WorkerID:        "worker-1",
		Kind:            "echo",
		Capacity:        capacity,
		InlineMax:       inlineMax,
		RetryInterval:   20 * time.Millisecond,
		DrainTimeout:    drainTimeout,
		ResolveInterval: 20 * time.Millisecond,
	}, resolver, hc, port(blobs), slog.New(slog.DiscardHandler))
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- s.Run(ctx) }()
	t.Cleanup(func() {
		cancel()
		s.Close()
	})
	return &harness{nodes: resolver, handler: h, done: done, cancel: cancel}
}

// port - nil *fakeBlobs as a nil interface: no storage.
func port(b *fakeBlobs) sidecar.Blobs {
	if b == nil {
		return nil
	}
	return b
}

// stop drains the sidecar and returns the result of Run.
func (h *harness) stop(t *testing.T) error {
	t.Helper()
	h.cancel()
	select {
	case err := <-h.done:
		return err
	case <-time.After(wait):
		t.Fatal("Run did not return")
		return nil
	}
}

func attempt(t *testing.T, id string, call handlertest.Call) *remotev1.Attempt {
	return &remotev1.Attempt{
		TaskId:  id,
		Number:  1,
		Meta:    map[string]string{"kind": "echo", "client.request-id": "r-" + id},
		Payload: handlertest.Encode(t, call),
	}
}

func TestAttemptsAreExecutedWithinCapacity(t *testing.T) {
	n := startNode(t)
	h := start(t, n, 2, wait)
	for i := range 6 {
		n.push(attempt(t, fmt.Sprint("t", i), handlertest.Call{Data: fmt.Sprint("d", i), SleepMs: 50}))
	}
	hello := n.hello(t)
	if hello.WorkerId != "worker-1" || hello.InstanceId == "" || hello.Kind != "echo" || hello.Capacity != 2 {
		t.Fatalf("hello %v", hello)
	}
	for range 6 {
		r := n.result(t)
		if r.Status != remotev1.Status_DONE || r.WorkerId != "worker-1" || r.Attempt != 1 {
			t.Fatalf("result %v", r)
		}
		reply := handlertest.Decode(t, r.GetBody())
		if want := "d" + r.TaskId[1:]; reply.Data != want {
			t.Fatalf("data %q, want %q", reply.Data, want)
		}
		if reply.Metadata["x-task-id"] != r.TaskId || reply.Metadata["x-meta-client.request-id"] != "r-"+r.TaskId {
			t.Fatalf("metadata %v", reply.Metadata)
		}
		// the meta of the attempt goes back: the node finds the task by it
		if r.Meta["client.request-id"] != "r-"+r.TaskId || r.Meta["kind"] != "echo" {
			t.Fatalf("meta of the result %v", r.Meta)
		}
	}
	if p := h.handler.Peak(); p != 2 {
		t.Fatalf("peak %d concurrent calls, want capacity 2", p)
	}
	if err := h.stop(t); err != nil {
		t.Fatal(err)
	}
}

func TestHandlerFailureIsReported(t *testing.T) {
	n := startNode(t)
	start(t, n, 1, wait)
	n.push(attempt(t, "t1", handlertest.Call{StatusCode: int32(codes.InvalidArgument)}))
	r := n.result(t)
	if r.Status != remotev1.Status_FAILED || r.Error == "" || len(r.GetBody()) > 0 {
		t.Fatalf("result %v", r)
	}
}

func TestCompleteIsRepeatedUntilOK(t *testing.T) {
	n := startNode(t)
	n.failCompletes.Store(2)
	start(t, n, 1, wait)
	n.push(attempt(t, "t1", handlertest.Call{}))
	if r := n.result(t); r.TaskId != "t1" {
		t.Fatalf("result %v", r)
	}
	if c := n.completeCalls.Load(); c != 3 {
		t.Fatalf("%d Complete calls, want 3", c)
	}
}

func TestRejectedCompleteIsNotRepeated(t *testing.T) {
	n := startNode(t)
	n.rejectComplete.Store(true)
	h := start(t, n, 1, wait)
	n.push(attempt(t, "t1", handlertest.Call{}))
	deadline := time.Now().Add(wait)
	for n.completeCalls.Load() == 0 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	time.Sleep(100 * time.Millisecond) // 5 retry intervals
	if c := n.completeCalls.Load(); c != 1 {
		t.Fatalf("%d Complete calls, want 1", c)
	}
	h.cancel()
}

func TestAttemptBeyondCreditIsRetriedWithoutCall(t *testing.T) {
	n := startNode(t)
	h := start(t, n, 1, wait)
	n.push(attempt(t, "t1", handlertest.Call{SleepMs: 200}))
	deadline := time.Now().Add(wait)
	for h.handler.Calls() == 0 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	n.rogue.Store(true) // the only slot is busy: the next attempt is beyond credit
	n.push(attempt(t, "t2", handlertest.Call{}))
	statuses := map[string]remotev1.Status{}
	for range 2 {
		r := n.result(t)
		statuses[r.TaskId] = r.Status
		if r.Meta["client.request-id"] != "r-"+r.TaskId {
			t.Fatalf("meta of the result %v", r.Meta)
		}
	}
	if statuses["t2"] != remotev1.Status_RETRY {
		t.Fatalf("statuses %v, want t2 RETRY", statuses)
	}
	if c := h.handler.Calls(); c != 1 {
		t.Fatalf("%d handler calls, want 1", c)
	}
}

func TestDrainFinishesAttemptInProgressAndExits(t *testing.T) {
	n := startNode(t)
	h := start(t, n, 1, wait)
	n.push(attempt(t, "t1", handlertest.Call{SleepMs: 200}))
	deadline := time.Now().Add(wait)
	for h.handler.Calls() == 0 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}

	h.cancel()
	select {
	case <-n.halfClosed:
	case <-time.After(wait):
		t.Fatal("no half-close")
	}
	n.push(attempt(t, "t2", handlertest.Call{})) // the node sends nothing after the half-close
	if r := n.result(t); r.TaskId != "t1" || r.Status != remotev1.Status_DONE {
		t.Fatalf("result %v", r)
	}
	select {
	case err := <-h.done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(wait):
		t.Fatal("Run did not return")
	}
	if c := h.handler.Calls(); c != 1 {
		t.Fatalf("%d handler calls, want 1", c)
	}
}

func TestDrainTimeoutAbandonsWork(t *testing.T) {
	n := startNode(t)
	h := start(t, n, 1, 200*time.Millisecond)
	n.push(attempt(t, "t1", handlertest.Call{SleepMs: 10_000}))
	deadline := time.Now().Add(wait)
	for h.handler.Calls() == 0 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if err := h.stop(t); !errors.Is(err, sidecar.ErrDrainTimeout) {
		t.Fatalf("err %v, want ErrDrainTimeout", err)
	}
}

// The same process reconnects with the same instance id: the node keeps its attempts in progress.
func TestReconnectsAfterNodeClosesSession(t *testing.T) {
	n := startNode(t)
	n.closeAtOnce.Store(true)
	start(t, n, 1, wait)
	first, second := n.hello(t), n.hello(t)
	if first.InstanceId == "" || second.InstanceId != first.InstanceId {
		t.Fatalf("instance ids %q, %q", first.InstanceId, second.InstanceId)
	}
}

// A session with each node: one pool of slots, each result to the node of its attempt, each node told its share.
func TestSessionWithEveryNodeSharesTheSlots(t *testing.T) {
	a, b := startNode(t), startNode(t)
	h := startWith(t, []*fakeNode{a, b}, 2, wait, nil)
	ha, hb := a.hello(t), b.hello(t)
	if ha.WorkerId != hb.WorkerId || ha.InstanceId != hb.InstanceId {
		t.Fatalf("hellos %v, %v", ha, hb)
	}
	a.awaitShare(t, 1)
	b.awaitShare(t, 1)
	for i := range 4 {
		a.push(attempt(t, fmt.Sprint("a", i), handlertest.Call{SleepMs: 30}))
		b.push(attempt(t, fmt.Sprint("b", i), handlertest.Call{SleepMs: 30}))
	}
	for _, n := range []*fakeNode{a, b} {
		for range 4 {
			if r := n.result(t); r.Status != remotev1.Status_DONE || r.TaskId[:1] != map[*fakeNode]string{a: "a", b: "b"}[n] {
				t.Fatalf("result %v at the wrong node", r)
			}
		}
	}
	if p := h.handler.Peak(); p > 2 {
		t.Fatalf("peak %d concurrent calls, want at most the capacity 2", p)
	}
	if err := h.stop(t); err != nil {
		t.Fatal(err)
	}
}

// A node gone from the address is not reconnected once its session ends.
func TestNodeGoneFromTheAddressIsNotReconnected(t *testing.T) {
	a, b := startNode(t), startNode(t)
	b.closeAtOnce.Store(true)
	h := startWith(t, []*fakeNode{a, b}, 1, wait, nil)
	b.hello(t)
	h.nodes.set(a)
	time.Sleep(100 * time.Millisecond) // the address is resolved again; a connect in flight ends
	for len(b.hellos) > 0 {
		<-b.hellos
	}
	select {
	case <-b.hellos:
		t.Fatal("reconnected to a node gone from the address")
	case <-time.After(1500 * time.Millisecond): // longer than the pause before a reconnect
	}
	a.awaitShare(t, 1)
}

// The node of the attempt is gone: the result goes to another node (a cluster finds the task by the meta).
func TestResultGoesToAnotherNodeWhenItsNodeIsGone(t *testing.T) {
	a, b := startNode(t), startNode(t)
	h := startWith(t, []*fakeNode{a, b}, 1, wait, nil)
	a.hello(t)
	b.hello(t)
	b.push(attempt(t, "t1", handlertest.Call{SleepMs: 300}))
	deadline := time.Now().Add(wait)
	for h.handler.Calls() == 0 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	h.nodes.set(a)
	b.srv.Stop()
	if r := a.result(t); r.TaskId != "t1" || r.Status != remotev1.Status_DONE {
		t.Fatalf("result %v", r)
	}
}

func TestAnswerUpToInlineMaxIsInline(t *testing.T) {
	n := startNode(t)
	blobs := &fakeBlobs{puts: map[string][]byte{}}
	startWith(t, []*fakeNode{n}, 1, wait, blobs)
	n.push(attempt(t, "t1", handlertest.Call{PadBytes: inlineMax / 2}))
	r := n.result(t)
	if r.Status != remotev1.Status_DONE || r.GetBlob() != nil || len(r.GetBody()) == 0 || len(blobs.puts) != 0 {
		t.Fatalf("result %v, %d puts", r, len(blobs.puts))
	}
}

func TestAnswerAboveInlineMaxGoesToBlobStorage(t *testing.T) {
	n := startNode(t)
	blobs := &fakeBlobs{puts: map[string][]byte{}}
	startWith(t, []*fakeNode{n}, 1, wait, blobs)
	n.push(attempt(t, "t1", handlertest.Call{Data: "big", PadBytes: 2 * inlineMax}))
	r := n.result(t)
	ref := r.GetBlob()
	if r.Status != remotev1.Status_DONE || ref == nil || len(r.GetBody()) != 0 {
		t.Fatalf("result %v", r)
	}
	stored := blobs.puts[ref.Location]
	if ref.Size != int64(len(stored)) || ref.Sha256 != "sha" || handlertest.Decode(t, stored).Data != "big" {
		t.Fatalf("ref %v, stored %d bytes", ref, len(stored))
	}
}

func TestAnswerAboveInlineMaxWithoutStorageFails(t *testing.T) {
	n := startNode(t)
	start(t, n, 1, wait)
	n.push(attempt(t, "t1", handlertest.Call{PadBytes: 2 * inlineMax}))
	r := n.result(t)
	if r.Status != remotev1.Status_FAILED || r.GetBlob() != nil || len(r.GetBody()) != 0 || r.Error == "" {
		t.Fatalf("result %v", r)
	}
}

func TestBlobStorageFailureIsRetried(t *testing.T) {
	n := startNode(t)
	blobs := &fakeBlobs{puts: map[string][]byte{}}
	blobs.fail.Store(true)
	startWith(t, []*fakeNode{n}, 1, wait, blobs)
	n.push(attempt(t, "t1", handlertest.Call{PadBytes: 2 * inlineMax}))
	r := n.result(t)
	if r.Status != remotev1.Status_RETRY || r.Error == "" {
		t.Fatalf("result %v", r)
	}
}
