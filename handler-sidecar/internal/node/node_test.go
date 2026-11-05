package node

import (
	"context"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"

	remotev1 "github.com/deripas/task-grid/handler-sidecar/internal/gen/remote/v1"
	"github.com/deripas/task-grid/handler-sidecar/internal/sidecar"
)

// A connection that stays open but carries nothing any more (a hung network path) breaks the session within
// keepalive time + timeout; a quiet but live connection keeps it. gRPC raises the time to 10s, so the test takes ~11s.
func TestKeepaliveBreaksAHungSession(t *testing.T) {
	if testing.Short() {
		t.Skip("waits for a keepalive ping (~11s)")
	}
	addr := quietNode(t)
	cfg := func(target string) Config {
		return Config{Address: target, KeepaliveTime: 10 * time.Second, KeepaliveTimeout: time.Second}
	}
	hung := newProxy(t, addr)
	live := newProxy(t, addr)
	hungSession := open(t, cfg(hung.addr))
	liveSession := open(t, cfg(live.addr))

	broken := make(chan error, 2)
	go func() { _, err := hungSession.Recv(); broken <- err }()
	go func() { _, err := liveSession.Recv(); broken <- err }()
	hung.freeze()

	select {
	case err := <-broken:
		if err == nil {
			t.Fatal("Recv returned without an error")
		}
	case <-time.After(20 * time.Second):
		t.Fatal("hung session not broken within 20s")
	}
	// the live one survived the same pings
	select {
	case err := <-broken:
		t.Fatalf("live session broken too: %v", err)
	case <-time.After(500 * time.Millisecond):
	}
}

func open(t *testing.T, cfg Config) sidecar.Session {
	t.Helper()
	c, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = c.Close() })
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	s, err := c.Open(ctx, sidecar.Hello{WorkerID: "w", Kind: "k", Capacity: 1})
	if err != nil {
		t.Fatal(err)
	}
	return s
}

// quietNode - a node that accepts sessions and never sends anything; it permits pings every 5s, as the real one.
func quietNode(t *testing.T) string {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	srv := grpc.NewServer(grpc.KeepaliveEnforcementPolicy(keepalive.EnforcementPolicy{
		MinTime:             5 * time.Second,
		PermitWithoutStream: true,
	}))
	remotev1.RegisterNodeServer(srv, quiet{})
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)
	return lis.Addr().String()
}

type quiet struct {
	remotev1.UnimplementedNodeServer
}

func (quiet) Connect(stream grpc.BidiStreamingServer[remotev1.SidecarMessage, remotev1.NodeMessage]) error {
	for {
		if _, err := stream.Recv(); err != nil {
			return nil
		}
	}
}

// proxy forwards TCP until frozen; then it keeps the connections open and passes nothing.
type proxy struct {
	addr    string
	frozen  atomic.Bool
	release chan struct{}
}

func newProxy(t *testing.T, target string) *proxy {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	p := &proxy{addr: lis.Addr().String(), release: make(chan struct{})}
	t.Cleanup(func() {
		close(p.release)
		_ = lis.Close()
	})
	go func() {
		for {
			in, err := lis.Accept()
			if err != nil {
				return
			}
			out, err := net.Dial("tcp", target)
			if err != nil {
				_ = in.Close()
				continue
			}
			go p.pipe(in, out)
			go p.pipe(out, in)
		}
	}()
	return p
}

func (p *proxy) freeze() {
	p.frozen.Store(true)
}

func (p *proxy) pipe(from, to net.Conn) {
	defer func() { _ = to.Close() }()
	buf := make([]byte, 32<<10)
	for {
		n, err := from.Read(buf)
		if p.frozen.Load() {
			<-p.release
			return
		}
		if n > 0 {
			if _, werr := to.Write(buf[:n]); werr != nil {
				return
			}
		}
		if err != nil {
			return
		}
	}
}
