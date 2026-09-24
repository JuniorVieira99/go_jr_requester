package jr_requester

import (
	"context"
	"errors"
	"net"
	"net/http/httptrace"
	"sync"
	"time"
)

// ------------
// Structs
// ------------

// BatchRequestsMetrics holds metrics for a batch of requests.
type BatchRequestsMetrics struct {
	// Request Metrics
	Successful      bool
	Failed          bool
	Redirected      bool
	Timeout         bool
	ConnectionError bool
	Retries         uint64
	Redirects       uint64
	TotalDuration   time.Duration
	ReqPerSecond    float64
}

// ConnMetrics holds metrics for a connection.
//
// The connection counters are sampled from net/http/httptrace and are therefore
// approximate: the transport reports a connection as idle only when the caller
// has drained and closed the response body, and HTTP/2 multiplexes many
// requests onto one connection, so AliveConnections tracks checked-out
// connections rather than open sockets.
type ConnMetrics struct {
	// Metadata
	CreatedAt  time.Time
	LastUsedAt time.Time
	// Request Metrics
	TotalRequests         uint64
	SuccessfulRequests    uint64
	FailedRequests        uint64
	TotalRetries          uint64
	TotalRedirects        uint64
	TotalTimeouts         uint64
	TotalConnectionErrors uint64
	// Connection Metrics
	AliveConnections  uint64
	IdleConnections   uint64
	ClosedConnections uint64
	// Mutex
	mtx sync.Mutex
}

// ------------
// Constructors
// ------------

func NewConnMetrics() *ConnMetrics {
	now := time.Now()
	return &ConnMetrics{
		CreatedAt:  now,
		LastUsedAt: now,
	}
}

// Clone returns a snapshot of the metrics. The copy carries its own mutex, so
// callers can read it without racing the connection that keeps recording.
func (cm *ConnMetrics) Clone() *ConnMetrics {
	if cm == nil {
		return nil
	}
	cm.mtx.Lock()
	defer cm.mtx.Unlock()

	return &ConnMetrics{
		CreatedAt:             cm.CreatedAt,
		LastUsedAt:            cm.LastUsedAt,
		TotalRequests:         cm.TotalRequests,
		SuccessfulRequests:    cm.SuccessfulRequests,
		FailedRequests:        cm.FailedRequests,
		TotalRetries:          cm.TotalRetries,
		TotalRedirects:        cm.TotalRedirects,
		TotalTimeouts:         cm.TotalTimeouts,
		TotalConnectionErrors: cm.TotalConnectionErrors,
		AliveConnections:      cm.AliveConnections,
		IdleConnections:       cm.IdleConnections,
		ClosedConnections:     cm.ClosedConnections,
	}
}

// ------------
// Methods
// ------------

func (cm *ConnMetrics) incrementAliveConnections() {
	cm.mtx.Lock()
	defer cm.mtx.Unlock()
	cm.AliveConnections++
}

func (cm *ConnMetrics) decrementAliveConnections() {
	cm.mtx.Lock()
	defer cm.mtx.Unlock()
	// These counters are unsigned; guard so an unmatched decrement cannot wrap
	// the value around to a huge number.
	if cm.AliveConnections > 0 {
		cm.AliveConnections--
	}
}

func (cm *ConnMetrics) incrementIdleConnections() {
	cm.mtx.Lock()
	defer cm.mtx.Unlock()
	cm.IdleConnections++
}

func (cm *ConnMetrics) decrementIdleConnections() {
	cm.mtx.Lock()
	defer cm.mtx.Unlock()
	if cm.IdleConnections > 0 {
		cm.IdleConnections--
	}
}

func (cm *ConnMetrics) incrementClosedConnections() {
	cm.mtx.Lock()
	defer cm.mtx.Unlock()
	cm.ClosedConnections++
}

func (cm *ConnMetrics) decrementClosedConnections() {
	cm.mtx.Lock()
	defer cm.mtx.Unlock()
	if cm.ClosedConnections > 0 {
		cm.ClosedConnections--
	}
}

// incrementRedirects records one followed redirect.
func (cm *ConnMetrics) incrementRedirects() {
	if cm == nil {
		return
	}
	cm.mtx.Lock()
	defer cm.mtx.Unlock()
	cm.TotalRedirects++
}

// incrementRetries records one replayed attempt.
func (cm *ConnMetrics) incrementRetries() {
	if cm == nil {
		return
	}
	cm.mtx.Lock()
	defer cm.mtx.Unlock()
	cm.TotalRetries++
}

// recordRequest tallies one finished request and classifies its failure.
func (cm *ConnMetrics) recordRequest(err error) {
	if cm == nil {
		return
	}
	cm.mtx.Lock()
	defer cm.mtx.Unlock()

	cm.LastUsedAt = time.Now()
	cm.TotalRequests++
	if err == nil {
		cm.SuccessfulRequests++
		return
	}

	cm.FailedRequests++
	var netErr net.Error
	switch {
	case errors.Is(err, context.DeadlineExceeded), errors.As(err, &netErr) && netErr.Timeout():
		cm.TotalTimeouts++
	default:
		cm.TotalConnectionErrors++
	}
}

// clientTrace samples connection reuse for a single request.
func (cm *ConnMetrics) clientTrace() *httptrace.ClientTrace {
	if cm == nil {
		return nil
	}
	return &httptrace.ClientTrace{
		GotConn: func(info httptrace.GotConnInfo) {
			cm.incrementAliveConnections()
			if info.Reused {
				cm.decrementIdleConnections()
			}
		},
		PutIdleConn: func(err error) {
			cm.decrementAliveConnections()
			if err == nil {
				cm.incrementIdleConnections()
			} else {
				// The connection could not be pooled and was closed instead.
				cm.incrementClosedConnections()
			}
		},
	}
}

func (cm *ConnMetrics) MarkAccessed(totalRequests uint64, successfulRequests uint64, failedRequests uint64, totalRetries uint64, totalRedirects uint64, totalTimeouts uint64, totalConnectionErrors uint64) {
	cm.mtx.Lock()
	defer cm.mtx.Unlock()
	cm.LastUsedAt = time.Now()
	cm.TotalRequests = totalRequests
	cm.SuccessfulRequests = successfulRequests
	cm.FailedRequests = failedRequests
	cm.TotalRetries = totalRetries
	cm.TotalRedirects = totalRedirects
	cm.TotalTimeouts = totalTimeouts
	cm.TotalConnectionErrors = totalConnectionErrors
}

func (cm *ConnMetrics) MarkConnectionStatus(aliveConnections uint64, idleConnections uint64, closedConnections uint64) {
	cm.mtx.Lock()
	defer cm.mtx.Unlock()
	cm.AliveConnections = aliveConnections
	cm.IdleConnections = idleConnections
	cm.ClosedConnections = closedConnections
}

func (cm *ConnMetrics) Reset() {
	cm.mtx.Lock()
	defer cm.mtx.Unlock()
	now := time.Now()
	cm.CreatedAt = now
	cm.LastUsedAt = now
	cm.TotalRequests = 0
	cm.SuccessfulRequests = 0
	cm.FailedRequests = 0
	cm.TotalRetries = 0
	cm.TotalRedirects = 0
	cm.TotalTimeouts = 0
	cm.TotalConnectionErrors = 0
	cm.AliveConnections = 0
	cm.IdleConnections = 0
	cm.ClosedConnections = 0
}
