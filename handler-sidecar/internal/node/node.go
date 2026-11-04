// Package node - adapter to the task-grid node: protocol taskgrid.worker.remote.v1 (internal/gen) <-> domain,
// gRPC codes -> errors of the port sidecar.Node.
package node

import (
	"context"
	"fmt"
	"sync"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	"github.com/deripas/task-grid/handler-sidecar/internal/domain"
	remotev1 "github.com/deripas/task-grid/handler-sidecar/internal/gen/remote/v1"
	"github.com/deripas/task-grid/handler-sidecar/internal/sidecar"
)

// Client - connection to one node.
type Client struct {
	conn *grpc.ClientConn
	stub remotev1.NodeClient
}

// New creates a client of the node at target (host:port); the connection is established lazily.
func New(target string) (*Client, error) {
	conn, err := grpc.NewClient(target, grpc.WithTransportCredentials(insecure.NewCredentials()))
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
		WorkerId: h.WorkerID,
		Kind:     h.Kind,
		Capacity: int32(h.Capacity),
	}}}
	if err := stream.Send(hello); err != nil {
		return nil, err
	}
	return &session{stream: stream}, nil
}

// Complete sends the result of an attempt; INVALID_ARGUMENT - sidecar.ErrRejected.
func (c *Client) Complete(ctx context.Context, workerID string, comp domain.Completion) error {
	_, err := c.stub.Complete(ctx, &remotev1.CompleteRequest{
		WorkerId: workerID,
		TaskId:   comp.TaskID,
		Attempt:  int32(comp.Attempt),
		Status:   toProto(comp.Status),
		Body:     comp.Body,
		Error:    comp.Error,
	})
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
