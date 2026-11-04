package credit

import (
	"slices"
	"testing"
)

func limit(t *testing.T, l *Ledger[string], k string) int64 {
	t.Helper()
	n, _, ok := l.Credit(k)
	if !ok {
		t.Fatalf("no account %s", k)
	}
	return n
}

func TestNoDemandNoCredit(t *testing.T) {
	l := New[string](2)
	l.Open("a")
	if got := l.Finish(); got != nil {
		t.Fatalf("granted %v without demand", got)
	}
	if n := limit(t, l, "a"); n != 0 {
		t.Fatalf("limit %d", n)
	}
}

func TestCreditBoundedBySlots(t *testing.T) {
	l := New[string](2)
	l.Open("a")
	if got := l.Demand("a", 5, 0); !slices.Equal(got, []string{"a"}) {
		t.Fatalf("changed %v", got)
	}
	if n, v, _ := l.Credit("a"); n != 2 || v != 1 {
		t.Fatalf("limit %d version %d, want 2 / 1", n, v)
	}
	// attempts hold the slots, credit is used up: no new credit until a slot is free
	l.Accept("a")
	l.Accept("a")
	if got := l.Demand("a", 5, 1); got != nil {
		t.Fatalf("changed %v with all slots busy", got)
	}
	if got := l.Finish(); !slices.Equal(got, []string{"a"}) || limit(t, l, "a") != 3 {
		t.Fatalf("after finish: changed %v, limit %d", got, limit(t, l, "a"))
	}
}

func TestLargestUnmetDemandFirst(t *testing.T) {
	l := New[string](3)
	l.Open("a")
	l.Open("b")
	l.Demand("a", 1, 0)
	l.Demand("b", 5, 0)
	if a, b := limit(t, l, "a"), limit(t, l, "b"); a != 1 || b != 2 {
		t.Fatalf("limits a=%d b=%d, want 1 / 2", a, b)
	}
}

func TestEqualDemandSharedNotOneNode(t *testing.T) {
	gotA := 0
	for range 200 {
		l := New[string](1)
		l.Open("a")
		l.Open("b")
		l.Stop() // demand of both before the grant
		l.Demand("a", 1, 0)
		l.Demand("b", 1, 0)
		l.stopped = false
		if got := l.grant(); len(got) != 1 {
			t.Fatalf("changed %v, want one node", got)
		}
		if limit(t, l, "a") == 1 {
			gotA++
		}
	}
	if gotA == 0 || gotA == 200 {
		t.Fatalf("node a got the slot %d of 200 times", gotA)
	}
}

func TestReturnedCreditGoesToOtherNode(t *testing.T) {
	l := New[string](1)
	l.Open("a")
	l.Open("b")
	l.Demand("a", 1, 0)
	l.Demand("b", 1, 0)
	if a := limit(t, l, "a"); a != 1 {
		t.Fatalf("limit a=%d", a)
	}
	// session a has seen the credit (version 1) and gave the attempt to another sidecar: demand 0 returns it to b
	if got := l.Demand("a", 0, 1); !slices.Equal(got, []string{"b"}) {
		t.Fatalf("changed %v", got)
	}
	if a, b := limit(t, l, "a"), limit(t, l, "b"); a != 0 || b != 1 {
		t.Fatalf("limits a=%d b=%d", a, b)
	}
}

func TestStaleDemandKeepsCredit(t *testing.T) {
	l := New[string](2)
	l.Open("a")
	l.Demand("a", 2, 0)
	// demand against version 0: the node has not seen the latest credit yet, it cannot be returned
	l.Demand("a", 0, 0)
	if a := limit(t, l, "a"); a != 2 {
		t.Fatalf("limit %d, want 2", a)
	}
}

func TestReturnNotBelowReceived(t *testing.T) {
	l := New[string](2)
	l.Open("a")
	l.Demand("a", 2, 0)
	l.Accept("a")
	l.Demand("a", 0, 1)
	if a := limit(t, l, "a"); a != 1 {
		t.Fatalf("limit %d, want 1 (one attempt received)", a)
	}
}

func TestAttemptBeyondCreditRejected(t *testing.T) {
	l := New[string](1)
	l.Open("a")
	l.Demand("a", 1, 0)
	if !l.Accept("a") {
		t.Fatal("attempt within credit rejected")
	}
	if l.Accept("a") {
		t.Fatal("attempt beyond credit accepted")
	}
	if l.Running() != 1 {
		t.Fatalf("running %d", l.Running())
	}
	if l.Accept("gone") {
		t.Fatal("attempt on closed account accepted")
	}
}

func TestCloseRedistributes(t *testing.T) {
	l := New[string](1)
	l.Open("a")
	l.Open("b")
	l.Demand("a", 1, 0)
	l.Demand("b", 1, 0)
	if got := l.Close("a"); !slices.Equal(got, []string{"b"}) {
		t.Fatalf("changed %v", got)
	}
	if _, _, ok := l.Credit("a"); ok {
		t.Fatal("closed account still has credit")
	}
}

func TestFreeSlotsOnTopOfRunning(t *testing.T) {
	l := New[string](3)
	l.Open("a")
	l.Demand("a", 10, 0)
	l.Accept("a")
	l.Accept("a")
	if a := limit(t, l, "a"); a != 3 {
		t.Fatalf("limit %d: 2 running + 1 free", a)
	}
}

func TestStopFreezesCredits(t *testing.T) {
	l := New[string](2)
	l.Open("a")
	l.Stop()
	if got := l.Demand("a", 2, 0); got != nil || limit(t, l, "a") != 0 {
		t.Fatalf("granted %v after stop", got)
	}
}
