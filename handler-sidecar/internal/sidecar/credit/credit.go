// Package credit - distribution of the slots of the sidecar among node sessions by their demand. Pure algorithm:
// no network and no locks, the caller holds its own mutex.
//
// A session may receive attempts with numbers <= limit (credit). Unused credit of all sessions plus attempts
// in progress <= slots. A free slot goes to the session with the largest unmet demand (target - limit),
// a random one among equals.
package credit

import (
	"math/rand/v2"
	"slices"
)

// Ledger - accounts of the sessions; K - key of a session.
type Ledger[K comparable] struct {
	slots    int64
	running  int64 // attempts holding a slot
	stopped  bool
	accounts map[K]*account
}

type account struct {
	limit    int64 // the node may send attempts with numbers <= limit
	version  int64 // grows with each new credit
	received int64 // attempts received
	target   int64 // demand of the node
}

// held - credit granted but not used yet.
func (a *account) held() int64 {
	return max(0, a.limit-a.received)
}

// New - a ledger of slots (the capacity of the handler).
func New[K comparable](slots int) *Ledger[K] {
	return &Ledger[K]{slots: int64(slots), accounts: map[K]*account{}}
}

// Open opens an account of a session: no credit until the node tells its demand.
func (l *Ledger[K]) Open(k K) {
	l.accounts[k] = &account{}
}

// Close closes an account: its unused credit goes to others. Returns the sessions with new credit.
func (l *Ledger[K]) Close(k K) []K {
	delete(l.accounts, k)
	return l.grant()
}

// Demand - the node tells its demand target, having seen the credit of version. It has seen the latest credit and
// wants less - surplus credit is returned (but not below what was received). Returns the sessions with new credit.
func (l *Ledger[K]) Demand(k K, target, version int64) []K {
	a, ok := l.accounts[k]
	if !ok {
		return nil
	}
	a.target = target
	if version == a.version && target < a.limit {
		a.limit = max(target, a.received)
	}
	return l.grant()
}

// Accept - an attempt came from the node. true - within credit, the attempt holds a slot; false - beyond credit
// (or the account is closed): it must not be executed.
func (l *Ledger[K]) Accept(k K) bool {
	a, ok := l.accounts[k]
	if !ok {
		return false
	}
	a.received++
	if a.received > a.limit {
		return false
	}
	l.running++
	return true
}

// Finish frees the slot of an attempt. Returns the sessions with new credit.
func (l *Ledger[K]) Finish() []K {
	l.running--
	return l.grant()
}

// Stop - grant no more credit (drain).
func (l *Ledger[K]) Stop() {
	l.stopped = true
}

// Credit - current credit of a session; ok == false - the account is closed.
func (l *Ledger[K]) Credit(k K) (limit, version int64, ok bool) {
	a, ok := l.accounts[k]
	if !ok {
		return 0, 0, false
	}
	return a.limit, a.version, true
}

// Running - attempts holding a slot.
func (l *Ledger[K]) Running() int {
	return int(l.running)
}

// grant distributes free slots; each session with new credit gets a new version.
func (l *Ledger[K]) grant() []K {
	if l.stopped {
		return nil
	}
	free := l.slots - l.running
	for _, a := range l.accounts {
		free -= a.held()
	}
	var changed []K
	for ; free > 0; free-- {
		var best K
		var bestAcc *account
		ties := 0
		for k, a := range l.accounts {
			unmet := a.target - a.limit
			if unmet <= 0 {
				continue
			}
			switch {
			case bestAcc == nil || unmet > bestAcc.target-bestAcc.limit:
				best, bestAcc, ties = k, a, 1
			case unmet == bestAcc.target-bestAcc.limit:
				// equal demand - a random session, not everything to one
				if ties++; rand.N(ties) == 0 {
					best, bestAcc = k, a
				}
			}
		}
		if bestAcc == nil {
			break
		}
		bestAcc.limit++
		if !slices.Contains(changed, best) {
			changed = append(changed, best)
		}
	}
	for _, k := range changed {
		l.accounts[k].version++
	}
	return changed
}
