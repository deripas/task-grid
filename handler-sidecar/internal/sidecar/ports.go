package sidecar

import (
	"context"
	"errors"

	"github.com/deripas/task-grid/handler-sidecar/internal/domain"
)

// ErrRejected - the node rejected the request itself: repeating it gives the same answer.
var ErrRejected = errors.New("request rejected")

// Nodes - port to the task-grid nodes behind one address (a DNS name of all the nodes of a cluster).
// Implementation - internal/node.
type Nodes interface {
	// Resolve - addresses (host:port) of the nodes now, each node once. An error - keep the known ones.
	Resolve(ctx context.Context) ([]string, error)
	// Dial - a client of the node at the address; the connection is established lazily.
	Dial(address string) (Node, error)
}

// Node - port to one task-grid node.
type Node interface {
	// Open opens a session: attempts and demand from the node, credit to the node. Cancelling ctx breaks the session.
	Open(ctx context.Context, h Hello) (Session, error)
	// Complete sends the result of an attempt. ErrRejected - it will never be accepted.
	Complete(ctx context.Context, workerID string, c domain.Completion) error
	// Close closes the connection.
	Close() error
}

// Session - an open session with the node. Recv runs in one goroutine, SendCredit and Drain in another.
type Session interface {
	// Recv - the next event; io.EOF - the node closed the session normally.
	Recv() (Event, error)
	// SendCredit - the node may send attempts with session numbers up to limit. Values are absolute.
	SendCredit(limit, version int64) error
	// SendShare - the share of the capacity the node counts in its load: capacity / number of sessions.
	SendShare(capacity float64) error
	// Drain - no more attempts wanted (half-close); the node closes the session after the results of the
	// attempts in progress.
	Drain() error
}

// Hello - who connects to the node.
type Hello struct {
	WorkerID   string
	InstanceID string // random, new on each start of the process: the node sees a restart that lost the attempts
	Kind       string
	Capacity   int
}

// Event - a message of the node: an attempt or demand; exactly one field is set.
type Event struct {
	Attempt *domain.Attempt
	Demand  *Demand
}

// Demand - how many attempts of the session the node would like to send in total (Target), having seen
// the credit of Version.
type Demand struct {
	Target  int64
	Version int64
}

// Handler - port to the handler: execute an attempt. Implementation - internal/handler.
type Handler interface {
	Call(ctx context.Context, a domain.Attempt) domain.Result
}

// Blobs - port to blob storage for answers above the inline limit. Implementation - internal/blob.
type Blobs interface {
	// Put stores the answer of an attempt of the task under a new key and returns the reference to it.
	Put(ctx context.Context, taskID string, data []byte) (domain.BlobRef, error)
}
