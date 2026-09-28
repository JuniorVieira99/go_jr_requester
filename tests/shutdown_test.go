package tests

import (
	"context"
	"errors"
	requester "github.com/JuniorVieira99/go_jr_requester/jr_requester"
	"net/http"
	"sync"
	"testing"
	"time"
)

// TestShutdownRejectsNewRequests checks the connection stays down once killed,
// instead of quietly dialling again the way Close allows.
func TestShutdownRejectsNewRequests(t *testing.T) {
	server := serve(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	conn := newTestConn(t, false, nil)

	resp, err := conn.GetRestHandler().Get(context.Background(), server.URL, nil)
	if err != nil {
		t.Fatalf("request before shutdown: %v", err)
	}
	resp.Close()

	if conn.IsShutdown() {
		t.Fatal("connection reports shut down before Shutdown was called")
	}
	conn.Shutdown()
	if !conn.IsShutdown() {
		t.Fatal("connection does not report shut down after Shutdown")
	}
	if got := conn.Status(); got != requester.ShutDown {
		t.Fatalf("status = %v, want ShutDown", got)
	}

	if _, err := conn.GetRestHandler().Get(context.Background(), server.URL, nil); !errors.Is(err, requester.ErrConnectionShutdown) {
		t.Fatalf("request after shutdown: err = %v, want ErrConnectionShutdown", err)
	}

	// A second call must not panic or double-cancel anything.
	conn.Shutdown()
}

// TestShutdownCancelsInFlight is the point of the kill switch: a request already
// parked on a slow server has to come back, not wait the server out.
func TestShutdownCancelsInFlight(t *testing.T) {
	release := make(chan struct{})
	started := make(chan struct{})
	var once sync.Once

	server := serve(t, func(w http.ResponseWriter, r *http.Request) {
		once.Do(func() { close(started) })
		select {
		case <-release:
		case <-r.Context().Done():
		}
		w.WriteHeader(http.StatusOK)
	})
	t.Cleanup(func() { close(release) })

	conn := newTestConn(t, false, nil)

	errc := make(chan error, 1)
	go func() {
		resp, err := conn.GetRestHandler().Get(context.Background(), server.URL, nil)
		if resp != nil {
			resp.Close()
		}
		errc <- err
	}()

	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("the handler never ran")
	}

	conn.Shutdown()

	select {
	case err := <-errc:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("in-flight request: err = %v, want context.Canceled", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Shutdown did not cancel the in-flight request")
	}
}

// TestShutdownCancelsBatch checks the cancellation reaches every goroutine of a
// concurrent batch, not just the one request DoRequest happens to be on.
func TestShutdownCancelsBatch(t *testing.T) {
	release := make(chan struct{})
	started := make(chan struct{}, 8)

	server := serve(t, func(w http.ResponseWriter, r *http.Request) {
		started <- struct{}{}
		select {
		case <-release:
		case <-r.Context().Done():
		}
		w.WriteHeader(http.StatusOK)
	})
	t.Cleanup(func() { close(release) })

	conn := newTestConn(t, false, func(s *requester.ConnSettings) {
		s.SetUseAsync(true)
		s.SetMaxConnsPerHost(4)
	})

	urls := []string{server.URL, server.URL, server.URL, server.URL}
	done := make(chan []requester.BatchResult, 1)
	go func() {
		done <- conn.GetRestHandler().BatchGet(context.Background(), urls, nil)
	}()

	for range urls {
		select {
		case <-started:
		case <-time.After(5 * time.Second):
			t.Fatal("not every batch request reached the handler")
		}
	}

	conn.Shutdown()

	select {
	case results := <-done:
		defer requester.CloseResults(results)
		for i, result := range results {
			if result.Err == nil {
				t.Fatalf("result %d succeeded, want a cancellation", i)
			}
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Shutdown did not unblock the batch")
	}
}

// TestClosingBodyReleasesInFlight guards the bookkeeping: a completed request
// must not leave its cancel behind, or a long-lived connection grows a map
// entry per request forever.
func TestClosingBodyReleasesInFlight(t *testing.T) {
	server := serve(t, func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("hello"))
	})
	conn := newTestConn(t, false, nil)
	defer conn.Shutdown()

	for range 20 {
		resp, err := conn.GetRestHandler().Get(context.Background(), server.URL, nil)
		if err != nil {
			t.Fatal(err)
		}
		body, err := resp.Text()
		if err != nil {
			t.Fatal(err)
		}
		if body != "hello" {
			t.Fatalf("body = %q", body)
		}
		resp.Close()
	}

	if n := conn.InFlight(); n != 0 {
		t.Fatalf("InFlight = %d after every body was closed, want 0", n)
	}
}

// TestBodyStaysReadableAfterDoRequest checks the per-request cancel outlives
// DoRequest: the body is read under the request context, so cancelling on
// return would break every read.
func TestBodyStaysReadableAfterDoRequest(t *testing.T) {
	server := serve(t, func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("streamed payload"))
	})
	conn := newTestConn(t, false, nil)
	defer conn.Shutdown()

	req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, server.URL, nil)
	if err != nil {
		t.Fatal(err)
	}
	raw, err := conn.DoRequest(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	defer raw.Body.Close()

	buf := make([]byte, 16)
	n, err := raw.Body.Read(buf)
	if err != nil && err.Error() != "EOF" {
		t.Fatalf("reading the body after DoRequest returned: %v", err)
	}
	if string(buf[:n]) != "streamed payload" {
		t.Fatalf("body = %q", string(buf[:n]))
	}
}
