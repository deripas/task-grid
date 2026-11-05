// Package sidecar - the core: sessions with the nodes, credit, execution of attempts, delivery of results, drain.
//
// The outside world is reached through ports (ports.go): Nodes, Handler and Blobs; implementations live in
// the adapters internal/node, internal/handler and internal/blob. Distribution of credit is a pure algorithm in the subpackage credit.
package sidecar

import (
	"context"
	crand "crypto/rand"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"maps"
	"math/rand/v2"
	"slices"
	"sync"
	"time"

	"github.com/deripas/task-grid/handler-sidecar/internal/domain"
	"github.com/deripas/task-grid/handler-sidecar/internal/sidecar/credit"
)

const callTimeout = 10 * time.Second

// ErrDrainTimeout - the work was not finished within DrainTimeout: attempts in progress are abandoned,
// their results are not delivered.
var ErrDrainTimeout = errors.New("drain timeout: work in progress abandoned")

// errDraining - the sidecar stops: no new sessions.
var errDraining = errors.New("draining")

// Config - settings of the core (see README).
type Config struct {
	WorkerID        string // stable name of the instance (pod name)
	Kind            string // kind of tasks the handler executes
	Capacity        int    // attempts the handler runs at once
	InlineMax       int    // bytes; a larger answer goes to blob storage
	RetryInterval   time.Duration
	DrainTimeout    time.Duration
	ResolveInterval time.Duration // how often the addresses of the nodes are resolved again
}

// Sidecar keeps a session with every node, grants them credit for its slots, calls the handler and delivers results.
//
//   - Nodes: the address is resolved every ResolveInterval; a session with each node. A new node - a session at once;
//     a node gone from the address keeps its session until it ends, without a reconnect.
//   - Session: one Connect stream; closed by the node or broken - reconnect (at once after a normal close
//     of a session older than a second, otherwise after a pause). Hello carries the instance id of the process:
//     the same in every session, so attempts in progress survive a reconnect.
//   - Credit: one pool of slots for all the sessions - unused credit plus attempts in progress <= Capacity,
//     by the demand of the nodes. An attempt beyond credit is not executed: its result is RETRY. Each node is told
//     its share of the capacity (Capacity / sessions) for its load metrics.
//   - An answer above InlineMax is put into blob storage within the slot of the attempt (memory stays bounded
//     by Capacity), the result carries the reference. No storage - FAILED, the storage fails - RETRY.
//   - A result goes to the node the attempt came from while that node is known; failed - the next tries go to the
//     other nodes too (any node of a cluster finds the task by the meta). Repeated until OK; rejected by the node
//     (INVALID_ARGUMENT) - given up.
//   - Drain (cancellation of the context of Run): no more credit, half-close of the streams; attempts in progress
//     are finished and delivered, the nodes close the streams; after DrainTimeout everything is abandoned.
type Sidecar struct {
	cfg        Config
	log        *slog.Logger
	nodes      Nodes
	handler    Handler
	blobs      Blobs // nil - no blob storage: answers above InlineMax fail
	instanceID string

	mu       sync.Mutex // links, sessions and credit
	credits  *credit.Ledger[*session]
	links    map[string]*link // address -> node
	sessions map[*session]struct{}
	draining bool
	drainCh  chan struct{}

	keeping sync.WaitGroup // keep loops of the links
	pending sync.WaitGroup // attempts in progress and delivery of results

	hard context.Context // cancelled when everything is abandoned
	stop context.CancelFunc
}

// link - a node at one address and the loop keeping a session with it.
type link struct {
	address string
	node    Node
	removed bool // gone from the address (under Sidecar.mu): no reconnect
}

// session - one session with a node; its credit is in Sidecar.credits (under Sidecar.mu).
type session struct {
	link   *link
	conn   Session
	opened time.Time
	done   chan error // nil - the node closed the session normally
	cancel context.CancelFunc
	wake   chan struct{} // there is new credit or a new share to send
	drain  chan struct{} // closed when the sidecar drains
}

// New creates a sidecar; blobs is nil when no blob storage is configured.
func New(cfg Config, nodes Nodes, handler Handler, blobs Blobs, log *slog.Logger) *Sidecar {
	s := &Sidecar{
		cfg:        cfg,
		log:        log,
		nodes:      nodes,
		handler:    handler,
		blobs:      blobs,
		instanceID: crand.Text(),
		credits:    credit.New[*session](cfg.Capacity),
		links:      map[string]*link{},
		sessions:   map[*session]struct{}{},
		drainCh:    make(chan struct{}),
	}
	s.hard, s.stop = context.WithCancel(context.Background())
	return s
}

// Run works until ctx is cancelled (SIGTERM), then drains and returns.
// ErrDrainTimeout - the drain did not fit into DrainTimeout.
func (s *Sidecar) Run(ctx context.Context) error {
	watchDone := make(chan struct{})
	go func() {
		defer close(watchDone)
		s.watch()
	}()
	<-ctx.Done()
	err := s.drainAndWait(watchDone)
	s.mu.Lock()
	for _, l := range s.links {
		_ = l.node.Close()
	}
	s.mu.Unlock()
	return err
}

// Close abandons all work.
func (s *Sidecar) Close() {
	s.stop()
}

// watch resolves the nodes until the drain, then waits for the keep loops. No node known yet - it tries again
// after a pause, not after the whole interval.
func (s *Sidecar) watch() {
	for {
		s.resolve()
		s.mu.Lock()
		next := s.cfg.ResolveInterval
		if len(s.links) == 0 {
			next = min(next, backoff())
		}
		s.mu.Unlock()
		select {
		case <-time.After(next):
		case <-s.drainCh:
			s.keeping.Wait()
			return
		}
	}
}

// resolve starts a keep loop for each new node and marks the nodes gone from the address.
func (s *Sidecar) resolve() {
	ctx, cancel := context.WithTimeout(s.hard, callTimeout)
	addrs, err := s.nodes.Resolve(ctx)
	cancel()
	if err != nil || len(addrs) == 0 {
		// a failure of DNS must not end the sessions that work
		s.log.Warn("nodes not resolved, keeping the known ones", "err", err)
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.draining {
		return
	}
	for _, l := range s.links {
		removed := !slices.Contains(addrs, l.address)
		if removed && !l.removed {
			s.log.Info("node gone from the address", "node", l.address)
		}
		l.removed = removed
	}
	for _, addr := range addrs {
		if _, ok := s.links[addr]; ok {
			continue
		}
		n, err := s.nodes.Dial(addr)
		if err != nil {
			s.log.Warn("node client", "node", addr, "err", err)
			continue
		}
		l := &link{address: addr, node: n}
		s.links[addr] = l
		s.keeping.Add(1)
		go s.keep(l)
	}
}

// keep holds a session with the node until the drain or until the node is gone from the address.
func (s *Sidecar) keep(l *link) {
	defer s.keeping.Done()
	for {
		if s.forgotten(l) {
			return
		}
		sess, err := s.open(l)
		if errors.Is(err, errDraining) {
			return
		}
		delay := time.Duration(0)
		if err != nil {
			s.log.Warn("connect failed", "node", l.address, "err", err)
			delay = backoff()
		} else {
			err = <-sess.done
			s.log.Info("session closed", "node", l.address, "err", err)
			if s.isDraining() {
				return
			}
			// OK - superseded or node stopping: reconnect at once, unless the session lived less than a second
			if err != nil || time.Since(sess.opened) < time.Second {
				delay = backoff()
			}
		}
		if !s.sleep(delay) {
			return
		}
	}
}

// forgotten - the node is gone from the address: it is forgotten and its client closed; results of its attempts go
// to the other nodes.
func (s *Sidecar) forgotten(l *link) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !l.removed {
		return false
	}
	delete(s.links, l.address)
	_ = l.node.Close()
	s.log.Info("node forgotten", "node", l.address)
	return true
}

// open opens a session with the node and starts reading attempts and sending credit.
func (s *Sidecar) open(l *link) (*session, error) {
	ctx, cancel := context.WithCancel(s.hard)
	conn, err := l.node.Open(ctx, Hello{
		WorkerID:   s.cfg.WorkerID,
		InstanceID: s.instanceID,
		Kind:       s.cfg.Kind,
		Capacity:   s.cfg.Capacity,
	})
	if err != nil {
		cancel()
		return nil, err
	}
	sess := &session{
		link:   l,
		conn:   conn,
		opened: time.Now(),
		done:   make(chan error, 1),
		cancel: cancel,
		wake:   make(chan struct{}, 1),
		drain:  make(chan struct{}),
	}
	s.mu.Lock()
	// the drain takes the sessions under mu: a session opened after it would get no half-close
	if s.draining {
		s.mu.Unlock()
		cancel()
		return nil, errDraining
	}
	s.sessions[sess] = struct{}{}
	s.credits.Open(sess)
	s.wakeAll() // the share of every session changed
	sessions := len(s.sessions)
	s.mu.Unlock()

	go s.read(sess)
	go s.sendCredits(ctx, sess)
	s.log.Info("session open", "node", l.address, "worker_id", s.cfg.WorkerID, "instance_id", s.instanceID,
		"kind", s.cfg.Kind, "capacity", s.cfg.Capacity, "sessions", sessions)
	return sess, nil
}

// read takes attempts and demand of the node while the session is open.
func (s *Sidecar) read(sess *session) {
	var err error
	for {
		var ev Event
		if ev, err = sess.conn.Recv(); err != nil {
			break
		}
		switch {
		case ev.Attempt != nil:
			s.accept(sess, *ev.Attempt)
		case ev.Demand != nil:
			s.mu.Lock()
			wake(s.credits.Demand(sess, ev.Demand.Target, ev.Demand.Version))
			s.mu.Unlock()
		}
	}
	if errors.Is(err, io.EOF) {
		err = nil
	}
	s.mu.Lock()
	delete(s.sessions, sess)
	wake(s.credits.Close(sess))
	s.wakeAll()
	s.mu.Unlock()
	sess.cancel()
	sess.done <- err
}

// sendCredits sends the latest credit and share of the session; values are absolute, intermediate ones may be
// skipped. On drain it half-closes the stream: Send and CloseSend stay in one goroutine.
func (s *Sidecar) sendCredits(ctx context.Context, sess *session) {
	var sentVersion int64
	var sentShare float64
	for {
		select {
		case <-ctx.Done():
			return
		case <-sess.drain:
			if err := sess.conn.Drain(); err != nil {
				s.log.Warn("half-close failed", "node", sess.link.address, "err", err)
				sess.cancel()
			}
			return
		case <-sess.wake:
		}
		s.mu.Lock()
		limit, version, ok := s.credits.Credit(sess)
		share := float64(s.cfg.Capacity) / float64(max(1, len(s.sessions)))
		s.mu.Unlock()
		if !ok {
			return
		}
		var err error
		if version != sentVersion {
			if err = sess.conn.SendCredit(limit, version); err == nil {
				sentVersion = version
			}
		}
		if err == nil && share != sentShare {
			if err = sess.conn.SendShare(share); err == nil {
				sentShare = share
			}
		}
		if err != nil {
			s.log.Warn("credit not sent", "node", sess.link.address, "err", err)
			sess.cancel()
			return
		}
	}
}

// accept - an attempt from the node: within credit - executed, beyond - RETRY without a call.
func (s *Sidecar) accept(sess *session, a domain.Attempt) {
	s.mu.Lock()
	ok := s.credits.Accept(sess)
	s.mu.Unlock()
	s.pending.Add(1)
	if !ok {
		s.log.Warn("attempt beyond credit", "task", a.TaskID, "attempt", a.Number)
		go func() {
			defer s.pending.Done()
			s.complete(sess.link, domain.Completion{
				TaskID:  a.TaskID,
				Attempt: a.Number,
				Meta:    a.Meta,
				Result:  domain.Result{Status: domain.Retry, Error: "attempt beyond the credit of the sidecar"},
			})
		}()
		return
	}
	go s.run(sess.link, a)
}

// run executes an attempt: the slot is free as soon as the answer is ready (delivery is outside the slot).
func (s *Sidecar) run(origin *link, a domain.Attempt) {
	defer s.pending.Done()
	started := time.Now()
	res := s.offload(a, s.handler.Call(s.hard, a))
	s.mu.Lock()
	wake(s.credits.Finish())
	s.mu.Unlock()
	s.log.Debug("attempt finished", "task", a.TaskID, "attempt", a.Number, "status", res.Status,
		"ms", time.Since(started).Milliseconds(), "bytes", len(res.Body), "blob", res.Blob != nil)
	s.complete(origin, domain.Completion{TaskID: a.TaskID, Attempt: a.Number, Meta: a.Meta, Result: res})
}

// offload puts an answer above InlineMax into blob storage: the result carries the reference instead of the body.
func (s *Sidecar) offload(a domain.Attempt, res domain.Result) domain.Result {
	if res.Status != domain.Done || len(res.Body) <= s.cfg.InlineMax {
		return res
	}
	if s.blobs == nil {
		return domain.Result{
			Status: domain.Failed,
			Error:  fmt.Sprintf("answer of %d bytes above the inline limit %d, no blob storage", len(res.Body), s.cfg.InlineMax),
		}
	}
	ref, err := s.blobs.Put(s.hard, a.TaskID, res.Body)
	if err != nil {
		s.log.Warn("blob not stored", "task", a.TaskID, "attempt", a.Number, "bytes", len(res.Body), "err", err)
		return domain.Result{Status: domain.Retry, Error: "blob storage: " + err.Error()}
	}
	return domain.Result{Status: domain.Done, Blob: &ref}
}

// complete delivers a result until OK; ErrRejected - it will never be accepted, no point in repeating.
func (s *Sidecar) complete(origin *link, c domain.Completion) {
	for attempt := 1; ; attempt++ {
		var err error
		if n := s.target(origin, attempt); n != nil {
			ctx, cancel := context.WithTimeout(s.hard, callTimeout)
			err = n.Complete(ctx, s.cfg.WorkerID, c)
			cancel()
		} else {
			err = errors.New("no nodes")
		}
		switch {
		case err == nil:
			return
		case errors.Is(err, ErrRejected):
			s.log.Error("result rejected", "task", c.TaskID, "attempt", c.Attempt, "status", c.Status, "err", err)
			return
		case s.hard.Err() != nil:
			s.log.Error("result lost on shutdown", "task", c.TaskID, "attempt", c.Attempt, "status", c.Status)
			return
		}
		s.log.Warn("complete failed", "task", c.TaskID, "try", attempt, "err", err)
		select {
		case <-time.After(s.cfg.RetryInterval):
		case <-s.hard.Done():
		}
	}
}

// target - the node for a try of delivering a result: the node of the attempt while it is known; after a failure the
// tries go round all the known nodes (the node of the attempt among them). nil - no nodes known.
func (s *Sidecar) target(origin *link, attempt int) Node {
	s.mu.Lock()
	defer s.mu.Unlock()
	// a node that is still there keeps its results: a node with a queue of its own would ignore them
	if known := s.links[origin.address] == origin; known && (attempt == 1 || !origin.removed) {
		return origin.node
	}
	if len(s.links) == 0 {
		return nil
	}
	addrs := slices.Sorted(maps.Keys(s.links))
	return s.links[addrs[attempt%len(addrs)]].node
}

// drainAndWait stops granting credit and half-closes the sessions; waits until the nodes close them and the results
// are delivered, at most DrainTimeout.
func (s *Sidecar) drainAndWait(watchDone <-chan struct{}) error {
	s.mu.Lock()
	s.draining = true
	s.credits.Stop()
	for sess := range s.sessions {
		close(sess.drain)
	}
	s.log.Info("draining", "running", s.credits.Running(), "sessions", len(s.sessions))
	s.mu.Unlock()
	close(s.drainCh)

	deadline := time.NewTimer(s.cfg.DrainTimeout)
	defer deadline.Stop()
	// attempts arrive only while the session is open: pending is waited for after it is over
	select {
	case <-watchDone:
	case <-deadline.C:
		return s.abandon(watchDone)
	}
	pendingDone := make(chan struct{})
	go func() {
		s.pending.Wait()
		close(pendingDone)
	}()
	select {
	case <-pendingDone:
		s.log.Info("drained")
		return nil
	case <-deadline.C:
		return s.abandon(watchDone)
	}
}

func (s *Sidecar) abandon(watchDone <-chan struct{}) error {
	s.mu.Lock()
	s.log.Error("drain timeout, abandoning work", "running", s.credits.Running())
	s.mu.Unlock()
	s.stop()
	<-watchDone
	s.pending.Wait()
	return ErrDrainTimeout
}

func (s *Sidecar) isDraining() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.draining
}

// sleep pauses before a reconnect; false - the sidecar drains, do not reconnect.
func (s *Sidecar) sleep(d time.Duration) bool {
	select {
	case <-time.After(d):
		return !s.isDraining()
	case <-s.drainCh:
		return false
	}
}

// wakeAll - the number of sessions changed: every session sends its new share. Under mu.
func (s *Sidecar) wakeAll() {
	for sess := range s.sessions {
		wake([]*session{sess})
	}
}

// wake wakes the credit senders of the sessions with new credit; does not block, may run under mu.
func wake(changed []*session) {
	for _, sess := range changed {
		select {
		case sess.wake <- struct{}{}:
		default:
		}
	}
}

func backoff() time.Duration {
	return 500*time.Millisecond + rand.N(500*time.Millisecond)
}
