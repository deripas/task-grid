// Package domain - types of the sidecar without protocols: an attempt from the node, the result of the handler call
// and the completion sent back. The protocol of the node is known only to the node adapter, the protocol of the
// handler - only to the handler adapter.
package domain

// Status - how an attempt ended.
type Status int

const (
	// Done - the handler answered; Result.Body is the answer. Domain errors (HTTP 404) are Done too.
	Done Status = iota + 1
	// Failed - final, a retry will not help.
	Failed
	// Retry - technical failure: the attempt counts, the task is retried.
	Retry
)

func (s Status) String() string {
	switch s {
	case Done:
		return "DONE"
	case Failed:
		return "FAILED"
	case Retry:
		return "RETRY"
	}
	return "UNKNOWN"
}

// Attempt - one execution of a task, as sent by the node.
type Attempt struct {
	TaskID  string
	Number  int               // attempt number of the task, from 1
	Meta    map[string]string // whole task meta
	Payload []byte            // input of the handler as is
}

// Result - outcome of a handler call.
type Result struct {
	Status Status
	Body   []byte // Done: answer of the handler as is
	Error  string // not Done: description of the failure
}

// Completion - result of an attempt for the node.
type Completion struct {
	TaskID  string
	Attempt int
	Result
}
