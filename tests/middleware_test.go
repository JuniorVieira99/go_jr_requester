package tests

import (
	"context"
	"errors"
	"fmt"
	requester "github.com/JuniorVieira99/go_jr_requester/jr_requester"
	"io"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
)

// TestMiddlewareOrderAndBothHalves checks a middleware sees the request going
// out and the response coming back, and that entries run outermost first.
func TestMiddlewareOrderAndBothHalves(t *testing.T) {
	server := serve(t, func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("body"))
	})

	var order []string
	var mtx sync.Mutex
	record := func(step string) {
		mtx.Lock()
		defer mtx.Unlock()
		order = append(order, step)
	}

	tag := func(name string) requester.Middleware {
		return func(req *http.Request, next requester.RoundTripFunc) (*http.Response, error) {
			record(name + ":out")
			resp, err := next(req)
			record(name + ":in")
			return resp, err
		}
	}

	conn, err := requester.NewConnection(1, requester.WithMiddleware(tag("first"), tag("second")))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Shutdown()

	resp, err := conn.GetRestHandler().Get(context.Background(), server.URL, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Close()

	got := strings.Join(order, ",")
	want := "first:out,second:out,second:in,first:in"
	if got != want {
		t.Fatalf("order = %q, want %q", got, want)
	}

	// The response has to survive the trip back out through the chain.
	if body, err := resp.Text(); err != nil || body != "body" {
		t.Fatalf("body = %q, err = %v", body, err)
	}
}

// TestMiddlewareCanSetHeaders checks the request handed to a middleware is a
// clone it may edit, and that the edit actually reaches the server.
func TestMiddlewareCanSetHeaders(t *testing.T) {
	seen := make(chan string, 1)
	server := serve(t, func(w http.ResponseWriter, r *http.Request) {
		seen <- r.Header.Get("X-Trace-Id")
	})

	conn, err := requester.NewConnection(1, requester.WithMiddleware(
		func(req *http.Request, next requester.RoundTripFunc) (*http.Response, error) {
			req.Header.Set("X-Trace-Id", "abc123")
			return next(req)
		},
	))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Shutdown()

	original, err := http.NewRequestWithContext(context.Background(), http.MethodGet, server.URL, nil)
	if err != nil {
		t.Fatal(err)
	}
	raw, err := conn.DoRequest(context.Background(), original)
	if err != nil {
		t.Fatal(err)
	}
	raw.Body.Close()

	if got := <-seen; got != "abc123" {
		t.Fatalf("X-Trace-Id = %q, want abc123", got)
	}
	// The caller's own request must come back untouched.
	if got := original.Header.Get("X-Trace-Id"); got != "" {
		t.Fatalf("the middleware mutated the caller's request: X-Trace-Id = %q", got)
	}
}

// TestMiddlewareShortCircuit covers the cache and circuit-breaker shape:
// returning a response without ever calling next.
func TestMiddlewareShortCircuit(t *testing.T) {
	var hits atomic.Int64
	server := serve(t, func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
	})

	conn, err := requester.NewConnection(1, requester.WithMiddleware(
		func(req *http.Request, next requester.RoundTripFunc) (*http.Response, error) {
			return &http.Response{
				StatusCode: http.StatusTeapot,
				Header:     make(http.Header),
				Body:       io.NopCloser(strings.NewReader("from the middleware")),
				Request:    req,
			}, nil
		},
	))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Shutdown()

	resp, err := conn.GetRestHandler().Get(context.Background(), server.URL, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Close()

	if resp.StatusCode() != http.StatusTeapot {
		t.Fatalf("status = %d, want 418", resp.StatusCode())
	}
	if body, _ := resp.Text(); body != "from the middleware" {
		t.Fatalf("body = %q", body)
	}
	if n := hits.Load(); n != 0 {
		t.Fatalf("the server was reached %d times, want 0", n)
	}
}

// TestMiddlewareRunsOncePerLogicalRequest pins the placement: the stack sits
// outside the retry layer, so three attempts are still one trip through it.
func TestMiddlewareRunsOncePerLogicalRequest(t *testing.T) {
	server := serve(t, func(w http.ResponseWriter, r *http.Request) {})
	// Point at a closed server so every attempt is a transport failure.
	server.Close()

	var passes atomic.Int64
	conn, err := requester.NewConnection(1,
		requester.WithRetries(2, 0),
		requester.WithMiddleware(func(req *http.Request, next requester.RoundTripFunc) (*http.Response, error) {
			passes.Add(1)
			return next(req)
		}),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Shutdown()

	if _, err := conn.GetRestHandler().Get(context.Background(), server.URL, nil); err == nil {
		t.Fatal("expected the request to fail against a closed server")
	}
	if n := passes.Load(); n != 1 {
		t.Fatalf("the middleware ran %d times, want 1 per logical request", n)
	}
}

// TestMiddlewareLiveEditing checks the stack can be changed on a built
// connection, which is the reason the chain is resolved per request.
func TestMiddlewareLiveEditing(t *testing.T) {
	server := serve(t, func(w http.ResponseWriter, r *http.Request) {})

	var calls atomic.Int64
	conn, err := requester.NewConnection(1)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Shutdown()

	stack := conn.GetMiddleware()
	if stack.Len() != 0 {
		t.Fatalf("Len = %d on a fresh connection, want 0", stack.Len())
	}

	id, err := stack.AddNamed("counter", func(req *http.Request, next requester.RoundTripFunc) (*http.Response, error) {
		calls.Add(1)
		return next(req)
	})
	if err != nil {
		t.Fatal(err)
	}
	if names := stack.Names(); len(names) != 1 || names[0] != "counter" {
		t.Fatalf("Names = %v", names)
	}

	get := func() {
		t.Helper()
		resp, err := conn.GetRestHandler().Get(context.Background(), server.URL, nil)
		if err != nil {
			t.Fatal(err)
		}
		resp.Close()
	}

	get()
	if calls.Load() != 1 {
		t.Fatalf("calls = %d after adding, want 1", calls.Load())
	}

	if err := stack.Remove(id); err != nil {
		t.Fatal(err)
	}
	get()
	if calls.Load() != 1 {
		t.Fatalf("calls = %d after removing, want it to stay at 1", calls.Load())
	}

	// Ids are not reused, so a stale removal is reported rather than silently
	// dropping whatever now sits at that position.
	if err := stack.Remove(id); !errors.Is(err, requester.ErrMiddlewareNotFound) {
		t.Fatalf("second Remove: err = %v, want ErrMiddlewareNotFound", err)
	}
}

// TestMiddlewareRejectsNil checks a nil registration is an error rather than a
// nil dereference on the next request.
func TestMiddlewareRejectsNil(t *testing.T) {
	if _, err := requester.NewConnection(1, requester.WithMiddleware(nil)); err == nil {
		t.Fatal("WithMiddleware(nil) built a connection, want an error")
	} else if !errors.Is(err, requester.ErrInvalidOption) {
		t.Fatalf("err = %v, want ErrInvalidOption", err)
	}

	conn, err := requester.NewConnection(1)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Shutdown()

	if _, err := conn.GetMiddleware().Add(nil); !errors.Is(err, requester.ErrNilMiddleware) {
		t.Fatalf("Add(nil): err = %v, want ErrNilMiddleware", err)
	}
}

// TestMiddlewareConcurrentEditing exercises the copy-on-write: requests keep
// running the chain they started with while the stack is edited underneath.
func TestMiddlewareConcurrentEditing(t *testing.T) {
	server := serve(t, func(w http.ResponseWriter, r *http.Request) {})
	conn := newTestConn(t, false, func(s *requester.ConnSettings) { s.SetUseAsync(true) })
	defer conn.Shutdown()

	stack := conn.GetMiddleware()
	pass := func(req *http.Request, next requester.RoundTripFunc) (*http.Response, error) {
		return next(req)
	}

	var wg sync.WaitGroup
	for i := range 20 {
		wg.Go(func() {
			id, err := stack.AddNamed(fmt.Sprintf("mw-%d", i), pass)
			if err != nil {
				t.Error(err)
				return
			}
			resp, err := conn.GetRestHandler().Get(context.Background(), server.URL, nil)
			if err != nil {
				t.Error(err)
				return
			}
			resp.Close()
			if err := stack.Remove(id); err != nil {
				t.Error(err)
			}
		})
	}
	wg.Wait()

	if n := stack.Len(); n != 0 {
		t.Fatalf("Len = %d after every add was removed, want 0", n)
	}
}
