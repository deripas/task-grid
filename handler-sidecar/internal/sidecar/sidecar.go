// Package sidecar - the core: a session with the node, credit, execution of attempts, delivery of results, drain.
//
// The outside world is reached through ports (ports.go): Node, Handler and Blobs; implementations live in
// the adapters internal/node, internal/handler and internal/blob. Distribution of credit is a pure algorithm in the subpackage credit.
package sidecar

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math/rand/v2"
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
	WorkerID      string // stable name of the instance (pod name)
	Kind          string // kind of tasks the handler executes
	Capacity      int    // attempts the handler runs at once
	InlineMax     int    // bytes; a larger answer goes to blob storage
	RetryInterval time.Duration
	DrainTimeout  time.Duration
}

// Sidecar keeps a session with the node, grants it credit for its slots, calls the handler and delivers results.
//
//   - Session: one Connect stream; closed by the node or broken - reconnect (at once after a normal close
//     of a session older than a second, otherwise after a pause).
//   - Credit: unused credit plus attempts in progress <= Capacity, by the demand of the node. An attempt beyond
//     credit is not executed: its result is RETRY.
//   - An answer above InlineMax is put into blob storage within the slot of the attempt (memory stays bounded
//     by Capacity), the result carries the reference. No storage - FAILED, the storage fails - RETRY.
//   - A result is repeated until OK; rejected by the node (INVALID_ARGUMENT) - given up.
//   - Drain (cancellation of the context of Run): no more credit, half-close of the stream; attempts in progress
//     are finished and delivered, the node closes the stream; after DrainTimeout everything is abandoned.
type Sidecar struct {
	cfg     Config
	log     *slog.Logger
	node    Node
	handler Handler
	blobs   Blobs // nil - no blob storage: answers above InlineMax fail

	mu       sync.Mutex // session and credit
	credits  *credit.Ledger[*session]
	current  *session
	draining bool
	drainCh  chan struct{}

	pending sync.WaitGroup // attempts in progress and delivery of results

	hard context.Context // cancelled when everything is abandoned
	stop context.CancelFunc
}

// session - one session with the node; its credit is in Sidecar.credits (under Sidecar.mu).
type session struct {
	link   Session
	opened time.Time
	done   chan error // nil - the node closed the session normally
	cancel context.CancelFunc
	wake   chan struct{} // there is new credit to send
	drain  chan struct{} // closed when the sidecar drains
}

// New creates a sidecar; blobs is nil when no blob storage is configured.
func New(cfg Config, node Node, handler Handler, blobs Blobs, log *slog.Logger) *Sidecar {
	s := &Sidecar{
		cfg:     cfg,
		log:     log,
		node:    node,
		handler: handler,
		blobs:   blobs,
		credits: credit.New[*session](cfg.Capacity),
		drainCh: make(chan struct{}),
	}
	s.hard, s.stop = context.WithCancel(context.Background())
	return s
}

// Run works until ctx is cancelled (SIGTERM), then drains and returns.
// ErrDrainTimeout - the drain did not fit into DrainTimeout.
func (s *Sidecar) Run(ctx context.Context) error {
	keepDone := make(chan struct{})
	go func() {
		defer close(keepDone)
		s.keep()
	}()
	<-ctx.Done()
	return s.drainAndWait(keepDone)
}

// Close abandons all work.
func (s *Sidecar) Close() {
	s.stop()
}

// keep holds a session with the node until the drain.
func (s *Sidecar) keep() {
	for {
		sess, err := s.open()
		if errors.Is(err, errDraining) {
			return
		}
		delay := time.Duration(0)
		if err != nil {
			s.log.Warn("connect failed", "err", err)
			delay = backoff()
		} else {
			err = <-sess.done
			s.log.Info("session closed", "err", err)
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

// open opens a session and starts reading attempts and sending credit.
func (s *Sidecar) open() (*session, error) {
	ctx, cancel := context.WithCancel(s.hard)
	link, err := s.node.Open(ctx, Hello{WorkerID: s.cfg.WorkerID, Kind: s.cfg.Kind, Capacity: s.cfg.Capacity})
	if err != nil {
		cancel()
		return nil, err
	}
	sess := &session{
		link:   link,
		opened: time.Now(),
		done:   make(chan error, 1),
		cancel: cancel,
		wake:   make(chan struct{}, 1),
		drain:  make(chan struct{}),
	}
	s.mu.Lock()
	// the drain takes the current session under mu: a session opened after it would get no half-close
	if s.draining {
		s.mu.Unlock()
		cancel()
		return nil, errDraining
	}
	s.current = sess
	s.credits.Open(sess)
	s.mu.Unlock()

	go s.read(sess)
	go s.sendCredits(ctx, sess)
	s.log.Info("session open", "worker_id", s.cfg.WorkerID, "kind", s.cfg.Kind, "capacity", s.cfg.Capacity)
	return sess, nil
}

// read takes attempts and demand of the node while the session is open.
func (s *Sidecar) read(sess *session) {
	var err error
	for {
		var ev Event
		if ev, err = sess.link.Recv(); err != nil {
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
	if s.current == sess {
		s.current = nil
	}
	wake(s.credits.Close(sess))
	s.mu.Unlock()
	sess.cancel()
	sess.done <- err
}

// sendCredits sends the latest credit of the session; values are absolute, intermediate ones may be skipped.
// On drain it half-closes the stream: Send and CloseSend stay in one goroutine.
func (s *Sidecar) sendCredits(ctx context.Context, sess *session) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-sess.drain:
			if err := sess.link.Drain(); err != nil {
				s.log.Warn("half-close failed", "err", err)
				sess.cancel()
			}
			return
		case <-sess.wake:
		}
		s.mu.Lock()
		limit, version, ok := s.credits.Credit(sess)
		s.mu.Unlock()
		if !ok {
			return
		}
		if err := sess.link.SendCredit(limit, version); err != nil {
			s.log.Warn("credit not sent", "err", err)
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
			s.complete(domain.Completion{
				TaskID:  a.TaskID,
				Attempt: a.Number,
				Result:  domain.Result{Status: domain.Retry, Error: "attempt beyond the credit of the sidecar"},
			})
		}()
		return
	}
	go s.run(a)
}

// run executes an attempt: the slot is free as soon as the answer is ready (delivery is outside the slot).
func (s *Sidecar) run(a domain.Attempt) {
	defer s.pending.Done()
	started := time.Now()
	res := s.offload(a, s.handler.Call(s.hard, a))
	s.mu.Lock()
	wake(s.credits.Finish())
	s.mu.Unlock()
	s.log.Debug("attempt finished", "task", a.TaskID, "attempt", a.Number, "status", res.Status,
		"ms", time.Since(started).Milliseconds(), "bytes", len(res.Body), "blob", res.Blob != nil)
	s.complete(domain.Completion{TaskID: a.TaskID, Attempt: a.Number, Result: res})
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
func (s *Sidecar) complete(c domain.Completion) {
	for attempt := 1; ; attempt++ {
		ctx, cancel := context.WithTimeout(s.hard, callTimeout)
		err := s.node.Complete(ctx, s.cfg.WorkerID, c)
		cancel()
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

// drainAndWait stops granting credit and half-closes the session; waits until the node closes it and the results
// are delivered, at most DrainTimeout.
func (s *Sidecar) drainAndWait(keepDone <-chan struct{}) error {
	s.mu.Lock()
	s.draining = true
	s.credits.Stop()
	if s.current != nil {
		close(s.current.drain)
	}
	s.log.Info("draining", "running", s.credits.Running())
	s.mu.Unlock()
	close(s.drainCh)

	deadline := time.NewTimer(s.cfg.DrainTimeout)
	defer deadline.Stop()
	// attempts arrive only while the session is open: pending is waited for after it is over
	select {
	case <-keepDone:
	case <-deadline.C:
		return s.abandon(keepDone)
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
		return s.abandon(keepDone)
	}
}

func (s *Sidecar) abandon(keepDone <-chan struct{}) error {
	s.mu.Lock()
	s.log.Error("drain timeout, abandoning work", "running", s.credits.Running())
	s.mu.Unlock()
	s.stop()
	<-keepDone
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
