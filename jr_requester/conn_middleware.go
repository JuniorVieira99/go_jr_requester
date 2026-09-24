package jr_requester

import (
	"errors"
	"fmt"
	"net/http"
	"slices"
	"sync"
)

// ------------
// Errors
// ------------

var (
	// ErrNilMiddleware is returned when a nil middleware is registered.
	ErrNilMiddleware = errors.New("nil middleware")
	// ErrMiddlewareNotFound is returned when removing an id that is not
	// registered, including one that was already removed.
	ErrMiddlewareNotFound = errors.New("middleware not found")
)

// ------------
// Types
// ------------

// RoundTripFunc is the next hop of a middleware chain: the rest of the chain,
// ending in the connection's transport.
type RoundTripFunc func(*http.Request) (*http.Response, error)

// RoundTrip lets a RoundTripFunc stand in for an http.RoundTripper.
func (f RoundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return f(req)
}

// Middleware observes or rewrites a request on its way out and the response on
// its way back. Both halves live in one function: whatever runs before the call
// to next happens on the way out, whatever runs after it happens on the way
// back, and next's return value is the response.
//
//	func timing(log *slog.Logger) requester.Middleware {
//		return func(req *http.Request, next requester.RoundTripFunc) (*http.Response, error) {
//			start := time.Now()
//			resp, err := next(req)
//			log.Info("request", "url", req.URL.Redacted(), "took", time.Since(start), "err", err)
//			return resp, err
//		}
//	}
//
// Two rules, both inherited from http.RoundTripper:
//
//   - Do not read or close the response body. It belongs to the caller, and a
//     middleware that drains it hands back an empty body.
//   - Returning without calling next short-circuits the request. That is a
//     legitimate way to build a cache or a circuit breaker, but the response
//     must be non-nil and carry a non-nil Body.
//
// The request handed in is already a clone, so setting headers on it directly
// is safe and does not disturb the caller's request.
type Middleware func(req *http.Request, next RoundTripFunc) (*http.Response, error)

// MiddlewareID identifies a registered middleware so it can be removed again.
// Ids are never reused, so removing a stale one reports
// ErrMiddlewareNotFound rather than dropping somebody else's middleware.
type MiddlewareID uint64

// middlewareEntry is one registration.
type middlewareEntry struct {
	id   MiddlewareID
	name string
	fn   Middleware
}

// MiddlewareStack is a connection's ordered list of middleware. It is resolved
// per request, so a middleware added after the connection is built takes effect
// on the next request rather than needing the transport rebuilt.
//
// The stack runs outside the retry layer, so a middleware sees one logical
// request no matter how many attempts it takes, and its response is the final
// one. It also runs before the cached headers and API key are applied, which
// keeps the key out of anything a logging middleware records.
//
//	client -> mw[0] -> mw[1] -> ... -> headers, API key, retries -> transport
//
// Every method is safe on a nil stack and safe to call from several goroutines,
// including from inside a request.
type MiddlewareStack struct {
	// mtx guards the fields below. entries is replaced, never edited in place,
	// so a request can keep running the snapshot it took while the stack is
	// being changed underneath it.
	mtx     sync.RWMutex
	entries []middlewareEntry
	nextID  MiddlewareID
}

// ------------
// Constructors
// ------------

// NewMiddlewareStack returns an empty stack. A Connection builds its own, so
// this is only needed to assemble one ahead of time.
func NewMiddlewareStack() *MiddlewareStack {
	return &MiddlewareStack{}
}

// ------------
// Methods
// ------------

// Add appends a middleware and returns the id that removes it again. The
// middleware added last runs innermost, closest to the transport.
func (s *MiddlewareStack) Add(mw Middleware) (MiddlewareID, error) {
	return s.AddNamed("", mw)
}

// AddNamed is Add with a label for Names, which is worth setting when the
// middleware is a closure and the stack is being inspected or logged.
func (s *MiddlewareStack) AddNamed(name string, mw Middleware) (MiddlewareID, error) {
	if s == nil {
		return 0, ErrNilMiddleware
	}
	if mw == nil {
		return 0, ErrNilMiddleware
	}

	s.mtx.Lock()
	defer s.mtx.Unlock()

	s.nextID++
	if name == "" {
		name = fmt.Sprintf("middleware-%d", s.nextID)
	}
	// Copy on write: a request may be part-way through the old slice.
	s.entries = append(slices.Clone(s.entries), middlewareEntry{id: s.nextID, name: name, fn: mw})
	return s.nextID, nil
}

// Remove drops the middleware with this id. Requests already running keep the
// chain they started with.
func (s *MiddlewareStack) Remove(id MiddlewareID) error {
	if s == nil {
		return ErrMiddlewareNotFound
	}

	s.mtx.Lock()
	defer s.mtx.Unlock()

	index := slices.IndexFunc(s.entries, func(e middlewareEntry) bool { return e.id == id })
	if index < 0 {
		return fmt.Errorf("%w: %d", ErrMiddlewareNotFound, id)
	}
	s.entries = slices.Delete(slices.Clone(s.entries), index, index+1)
	return nil
}

// Clear removes every middleware.
func (s *MiddlewareStack) Clear() {
	if s == nil {
		return
	}
	s.mtx.Lock()
	defer s.mtx.Unlock()
	s.entries = nil
}

// Len reports how many middleware are registered.
func (s *MiddlewareStack) Len() int {
	if s == nil {
		return 0
	}
	s.mtx.RLock()
	defer s.mtx.RUnlock()
	return len(s.entries)
}

// Names lists the registered middleware in the order they run.
func (s *MiddlewareStack) Names() []string {
	if s == nil {
		return nil
	}
	s.mtx.RLock()
	defer s.mtx.RUnlock()

	names := make([]string, len(s.entries))
	for i, entry := range s.entries {
		names[i] = entry.name
	}
	return names
}

// snapshot takes the current chain. The slice is never edited in place, so the
// caller can hold it for the length of a request.
func (s *MiddlewareStack) snapshot() []middlewareEntry {
	if s == nil {
		return nil
	}
	s.mtx.RLock()
	defer s.mtx.RUnlock()
	return s.entries
}

// ------------
// Transport
// ------------

// middlewareTransport runs the stack in front of base. It resolves the chain
// per request so the stack stays editable on a live connection.
type middlewareTransport struct {
	stack *MiddlewareStack
	base  http.RoundTripper
}

// wrap puts the stack in front of base. It always wraps, even while the stack
// is empty, because middleware can be added later.
func (s *MiddlewareStack) wrap(base http.RoundTripper) http.RoundTripper {
	if s == nil {
		return base
	}
	return &middlewareTransport{stack: s, base: base}
}

func (t *middlewareTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	entries := t.stack.snapshot()
	if len(entries) == 0 {
		return t.base.RoundTrip(req)
	}

	// Build the chain back to front, so entries[0] ends up outermost.
	next := RoundTripFunc(t.base.RoundTrip)
	for i := len(entries) - 1; i >= 0; i-- {
		mw, downstream := entries[i].fn, next
		next = func(r *http.Request) (*http.Response, error) {
			return mw(r, downstream)
		}
	}

	// A RoundTripper must not mutate the request it is handed. Cloning once
	// here means every middleware can edit headers without each having to.
	// DoRequest's requests are already private copies, so they go as they are.
	if !ownsRequest(req) {
		req = req.Clone(req.Context())
	}
	return next(req)
}
