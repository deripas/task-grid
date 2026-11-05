// Package node - adapter to the task-grid nodes: addresses of the nodes from DNS, protocol taskgrid.worker.remote.v1
// (internal/gen) <-> domain, gRPC codes -> errors of the port sidecar.Node.
package node

import (
	"context"
	"fmt"
	"net"
	"net/netip"
	"slices"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/status"

	"github.com/deripas/task-grid/handler-sidecar/internal/domain"
	remotev1 "github.com/deripas/task-grid/handler-sidecar/internal/gen/remote/v1"
	"github.com/deripas/task-grid/handler-sidecar/internal/sidecar"
)

// Config - the nodes and the check of the connections to them.
type Config struct {
	// host:port of the task-grid nodes: a DNS name resolved to every node (k8s headless service, network alias),
	// or the address of one node
	Address string
	// HTTP/2 pings, also without calls: a connection without an answer to a ping within KeepaliveTimeout is closed,
	// the session breaks and the sidecar reconnects. gRPC raises KeepaliveTime below 10s to 10s; the node must
	// permit pings that often.
	KeepaliveTime    time.Duration
	KeepaliveTimeout time.Duration
}

// Pool - the nodes behind Config.Address: implements the port sidecar.Nodes.
type Pool struct {
	cfg  Config
	host string
	port string
}

// NewPool checks the address; nothing is resolved yet.
func NewPool(cfg Config) (*Pool, error) {
	host, port, err := net.SplitHostPort(cfg.Address)
	if err != nil {
		return nil, fmt.Errorf("node address %q: %w", cfg.Address, err)
	}
	return &Pool{cfg: cfg, host: host, port: port}, nil
}

// Resolve - the addresses of the nodes, sorted. IPv4 ones if there are any, otherwise IPv6: a dual-stack name lists
// each node twice, and two sessions of one sidecar with one node would supersede each other.
func (p *Pool) Resolve(ctx context.Context) ([]string, error) {
	if _, err := netip.ParseAddr(p.host); err == nil {
		return []string{p.cfg.Address}, nil
	}
	ips, err := net.DefaultResolver.LookupNetIP(ctx, "ip4", p.host)
	if err != nil || len(ips) == 0 {
		if ips, err = net.DefaultResolver.LookupNetIP(ctx, "ip6", p.host); err != nil {
			return nil, err
		}
	}
	addrs := make([]string, 0, len(ips))
	for _, ip := range ips {
		addrs = append(addrs, net.JoinHostPort(ip.Unmap().String(), p.port))
	}
	slices.Sort(addrs)
	return slices.Compact(addrs), nil
}

// Dial - a client of the node at the address, with the keepalive of the pool.
func (p *Pool) Dial(address string) (sidecar.Node, error) {
	cfg := p.cfg
	cfg.Address = address
	return New(cfg)
}

// Client - connection to one node.
type Client struct {
	conn *grpc.ClientConn
	stub remotev1.NodeClient
}

// New creates a client of the node at cfg.Address; the connection is established lazily.
func New(cfg Config) (*Client, error) {
	conn, err := grpc.NewClient(cfg.Address,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithKeepaliveParams(keepalive.ClientParameters{
			Time:                cfg.KeepaliveTime,
			Timeout:             cfg.KeepaliveTimeout,
			PermitWithoutStream: true,
		}))
	if err != nil {
		return nil, err
	}
	return &Client{conn: conn, stub: remotev1.NewNodeClient(conn)}, nil
}

func (c *Client) Close() error {
	return c.conn.Close()
}

// Open starts the Connect stream and sends Hello.
func (c *Client) Open(ctx context.Context, h sidecar.Hello) (sidecar.Session, error) {
	stream, err := c.stub.Connect(ctx)
	if err != nil {
		return nil, err
	}
	hello := &remotev1.SidecarMessage{Msg: &remotev1.SidecarMessage_Hello{Hello: &remotev1.Hello{
		WorkerId:   h.WorkerID,
		InstanceId: h.InstanceID,
		Kind:       h.Kind,
		Capacity:   int32(h.Capacity),
	}}}
	if err := stream.Send(hello); err != nil {
		return nil, err
	}
	return &session{stream: stream}, nil
}

// Complete sends the result of an attempt; INVALID_ARGUMENT - sidecar.ErrRejected.
func (c *Client) Complete(ctx context.Context, workerID string, comp domain.Completion) error {
	req := &remotev1.CompleteRequest{
		WorkerId: workerID,
		TaskId:   comp.TaskID,
		Attempt:  int32(comp.Attempt),
		Status:   toProto(comp.Status),
		Error:    comp.Error,
		Meta:     comp.Meta,
	}
	if b := comp.Blob; b != nil {
		req.Result = &remotev1.CompleteRequest_Blob{Blob: &remotev1.BlobRef{
			Location: b.Location,
			Size:     b.Size,
			Sha256:   b.Sha256,
			Encoding: b.Encoding,
		}}
	} else if comp.Body != nil {
		req.Result = &remotev1.CompleteRequest_Body{Body: comp.Body}
	}
	_, err := c.stub.Complete(ctx, req)
	if status.Code(err) == codes.InvalidArgument {
		return fmt.Errorf("%w: %v", sidecar.ErrRejected, err)
	}
	return err
}

// session - one Connect stream. Send and CloseSend are serialized: gRPC forbids calling them concurrently.
type session struct {
	stream grpc.BidiStreamingClient[remotev1.SidecarMessage, remotev1.NodeMessage]
	mu     sync.Mutex
}

func (s *session) Recv() (sidecar.Event, error) {
	for {
		msg, err := s.stream.Recv()
		if err != nil {
			return sidecar.Event{}, err // io.EOF - the node closed the stream with OK
		}
		switch m := msg.Msg.(type) {
		case *remotev1.NodeMessage_Attempt:
			return sidecar.Event{Attempt: &domain.Attempt{
				TaskID:  m.Attempt.GetTaskId(),
				Number:  int(m.Attempt.GetNumber()),
				Meta:    m.Attempt.GetMeta(),
				Payload: m.Attempt.GetPayload(),
			}}, nil
		case *remotev1.NodeMessage_Demand:
			return sidecar.Event{Demand: &sidecar.Demand{
				Target:  m.Demand.GetTarget(),
				Version: m.Demand.GetVersion(),
			}}, nil
		}
		// a message unknown to this version of the protocol: skip it
	}
}

func (s *session) SendCredit(limit, version int64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.stream.Send(&remotev1.SidecarMessage{Msg: &remotev1.SidecarMessage_Credit{Credit: &remotev1.Credit{
		Limit:   limit,
		Version: version,
	}}})
}

func (s *session) SendShare(capacity float64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.stream.Send(&remotev1.SidecarMessage{Msg: &remotev1.SidecarMessage_Share{Share: &remotev1.Share{
		Capacity: capacity,
	}}})
}

func (s *session) Drain() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.stream.CloseSend()
}

func toProto(s domain.Status) remotev1.Status {
	switch s {
	case domain.Done:
		return remotev1.Status_DONE
	case domain.Failed:
		return remotev1.Status_FAILED
	case domain.Retry:
		return remotev1.Status_RETRY
	}
	return remotev1.Status_STATUS_UNSPECIFIED
}
