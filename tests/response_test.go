package tests

import (
	"bytes"
	"context"
	"errors"
	"io"
	requester "jr_requester/jr_requester"
	"log"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
)

func TestResponseBodyHelpers(t *testing.T) {
	server := serve(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json; charset=utf-8")
		w.Write([]byte(`{"name":"jr","count":3}`))
	})

	c := newTestConn(t, false, nil)
	resp, err := c.GetRestHandler().Get(context.Background(), server.URL, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Close()

	if !resp.IsSuccess() || resp.StatusCode() != 200 {
		t.Fatalf("status = %d", resp.StatusCode())
	}
	if resp.ContentType() != "application/json" {
		t.Fatalf("ContentType = %q, want the media type without parameters", resp.ContentType())
	}

	var payload struct {
		Name  string `json:"name"`
		Count int    `json:"count"`
	}
	if err := resp.JSON(&payload); err != nil {
		t.Fatal(err)
	}
	if payload.Name != "jr" || payload.Count != 3 {
		t.Fatalf("payload = %+v", payload)
	}

	// The body is cached, so a second read still works after JSON consumed it.
	text, err := resp.Text()
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(text, `"name":"jr"`) {
		t.Fatalf("Text() = %q", text)
	}
	body, err := resp.Bytes()
	if err != nil || len(body) == 0 {
		t.Fatalf("Bytes() = %q, %v", body, err)
	}

	// Close is idempotent.
	if err := resp.Close(); err != nil {
		t.Fatal(err)
	}
	if err := resp.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestResponseErrorForStatus(t *testing.T) {
	server := serve(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusUnprocessableEntity)
		w.Write([]byte("field \"name\"\nis required"))
	})

	c := newTestConn(t, false, nil)
	resp, err := c.GetRestHandler().Get(context.Background(), server.URL, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Close()

	// A 4xx is not a transport error, so err is nil and the status carries it.
	if resp.IsSuccess() || !resp.IsClientError() || !resp.IsError() {
		t.Fatalf("status classification wrong for %d", resp.StatusCode())
	}

	statusErr := resp.ErrorForStatus()
	if statusErr == nil {
		t.Fatal("expected an error for 422")
	}
	var typed *requester.StatusError
	if !errors.As(statusErr, &typed) {
		t.Fatalf("error is not a *requester.StatusError: %T", statusErr)
	}
	if typed.StatusCode != http.StatusUnprocessableEntity {
		t.Fatalf("StatusCode = %d", typed.StatusCode)
	}
	// The snippet must quote the body on one line so logs stay readable.
	if !strings.Contains(typed.Snippet, "is required") {
		t.Fatalf("Snippet = %q", typed.Snippet)
	}
	if strings.ContainsAny(typed.Error(), "\n") {
		t.Fatalf("error spans multiple lines: %q", typed.Error())
	}

	// A 2xx yields no error.
	ok := serve(t, func(w http.ResponseWriter, r *http.Request) {})
	good, err := c.GetRestHandler().Get(context.Background(), ok.URL, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer good.Close()
	if err := good.ErrorForStatus(); err != nil {
		t.Fatalf("ErrorForStatus on 200 = %v", err)
	}
}

func TestResponseBodyLimit(t *testing.T) {
	payload := bytes.Repeat([]byte("x"), 4096)
	server := serve(t, func(w http.ResponseWriter, r *http.Request) {
		w.Write(payload)
	})

	// Under the limit: fine.
	c := newTestConn(t, false, func(s *requester.ConnSettings) { s.SetMaxResponseBodySize(8192) })
	resp, err := c.GetRestHandler().Get(context.Background(), server.URL, nil)
	if err != nil {
		t.Fatal(err)
	}
	body, err := resp.Bytes()
	resp.Close()
	if err != nil {
		t.Fatal(err)
	}
	if len(body) != len(payload) {
		t.Fatalf("len(body) = %d, want %d", len(body), len(payload))
	}

	// Exactly at the limit is still allowed.
	exact := newTestConn(t, false, func(s *requester.ConnSettings) { s.SetMaxResponseBodySize(int64(len(payload))) })
	resp, err = exact.GetRestHandler().Get(context.Background(), server.URL, nil)
	if err != nil {
		t.Fatal(err)
	}
	body, err = resp.Bytes()
	resp.Close()
	if err != nil {
		t.Fatalf("a body exactly at the limit was rejected: %v", err)
	}
	if len(body) != len(payload) {
		t.Fatalf("len(body) = %d", len(body))
	}

	// Over the limit: refused, and the error says so.
	small := newTestConn(t, false, func(s *requester.ConnSettings) { s.SetMaxResponseBodySize(128) })
	resp, err = small.GetRestHandler().Get(context.Background(), server.URL, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Close()
	if _, err := resp.Bytes(); !errors.Is(err, requester.ErrBodyTooLarge) {
		t.Fatalf("err = %v, want requester.ErrBodyTooLarge", err)
	}
	// The failure is sticky rather than half-read on a retry.
	if _, err := resp.Text(); !errors.Is(err, requester.ErrBodyTooLarge) {
		t.Fatalf("second read err = %v", err)
	}

	// A negative limit means unlimited.
	unlimited := newTestConn(t, false, func(s *requester.ConnSettings) { s.SetMaxResponseBodySize(-1) })
	resp, err = unlimited.GetRestHandler().Get(context.Background(), server.URL, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Close()
	if body, err := resp.Bytes(); err != nil || len(body) != len(payload) {
		t.Fatalf("unlimited read: %d bytes, %v", len(body), err)
	}
}

func TestResponseStreamAndSave(t *testing.T) {
	server := serve(t, func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("streamed payload"))
	})

	// Save bypasses the buffer limit entirely.
	c := newTestConn(t, false, func(s *requester.ConnSettings) { s.SetMaxResponseBodySize(4) })
	resp, err := c.GetRestHandler().Get(context.Background(), server.URL, nil)
	if err != nil {
		t.Fatal(err)
	}
	var sink bytes.Buffer
	written, err := resp.Save(&sink)
	resp.Close()
	if err != nil {
		t.Fatal(err)
	}
	if sink.String() != "streamed payload" || written != 16 {
		t.Fatalf("Save wrote %d bytes: %q", written, sink.String())
	}

	// Streaming twice is refused rather than silently returning nothing.
	resp, err = c.GetRestHandler().Get(context.Background(), server.URL, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Close()
	stream, err := resp.Stream()
	if err != nil {
		t.Fatal(err)
	}
	io.Copy(io.Discard, stream)
	stream.Close()
	if _, err := resp.Stream(); !errors.Is(err, requester.ErrBodyConsumed) {
		t.Fatalf("second Stream err = %v, want requester.ErrBodyConsumed", err)
	}
	if _, err := resp.Bytes(); !errors.Is(err, requester.ErrBodyConsumed) {
		t.Fatalf("Bytes after Stream err = %v, want requester.ErrBodyConsumed", err)
	}

	// Buffer first, then Stream: served from the cache.
	buffered := newTestConn(t, false, nil)
	resp, err = buffered.GetRestHandler().Get(context.Background(), server.URL, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Close()
	if _, err := resp.Bytes(); err != nil {
		t.Fatal(err)
	}
	stream, err = resp.Stream()
	if err != nil {
		t.Fatal(err)
	}
	defer stream.Close()
	replayed, _ := io.ReadAll(stream)
	if string(replayed) != "streamed payload" {
		t.Fatalf("replayed = %q", replayed)
	}
}

// TestResponseCloseReturnsConnection is the point of the whole wrapper: closing
// without reading must still put the connection back in the pool.
func TestResponseCloseReturnsConnection(t *testing.T) {
	server, conns := serveCountingConns(t, func(w http.ResponseWriter, r *http.Request) {
		w.Write(bytes.Repeat([]byte("y"), 2048))
	})

	c := newTestConn(t, false, nil)
	for range 5 {
		resp, err := c.GetRestHandler().Get(context.Background(), server.URL, nil)
		if err != nil {
			t.Fatal(err)
		}
		// Never read the body, only close it.
		if err := resp.Close(); err != nil {
			t.Fatal(err)
		}
	}

	if n := conns.Load(); n != 1 {
		t.Fatalf("opened %d connections for 5 requests; Close did not drain the body", n)
	}
}

func TestNilResponseIsSafe(t *testing.T) {
	var resp *requester.Response

	// Accessors on a nil Response must not panic.
	if resp.StatusCode() != 0 || resp.Status() != "" || resp.ContentType() != "" {
		t.Fatal("nil accessors returned unexpected values")
	}
	if resp.Header() == nil {
		t.Fatal("Header() must never be nil")
	}
	if resp.IsSuccess() || resp.IsError() || resp.IsRedirect() {
		t.Fatal("nil response classified as something")
	}
	if resp.Raw() != nil || resp.Request() != nil || resp.Cookies() != nil {
		t.Fatal("nil response returned non-nil internals")
	}
	if err := resp.Close(); err != nil {
		t.Fatalf("Close on nil = %v", err)
	}
	if _, err := resp.Bytes(); !errors.Is(err, requester.ErrNilResponse) {
		t.Fatalf("Bytes on nil = %v", err)
	}
	if err := resp.ErrorForStatus(); !errors.Is(err, requester.ErrNilResponse) {
		t.Fatalf("ErrorForStatus on nil = %v", err)
	}
	if !strings.Contains(resp.String(), "nil") {
		t.Fatalf("String on nil = %q", resp.String())
	}

	// requester.CloseResults must tolerate failed entries whose Response is nil.
	requester.CloseResults([]requester.BatchResult{{Err: errors.New("boom")}, {}})
}

func TestBatchResponseHelpers(t *testing.T) {
	server := serve(t, func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(r.URL.Path))
	})

	c := newTestConn(t, false, func(s *requester.ConnSettings) { s.SetUseAsync(true) })
	urls := []string{server.URL + "/one", server.URL + "/two"}

	results := c.GetRestHandler().BatchGet(context.Background(), urls, nil)
	defer requester.CloseResults(results)

	if err := requester.FirstError(results); err != nil {
		t.Fatal(err)
	}
	if errs := requester.Errors(results); len(errs) != 0 {
		t.Fatalf("Errors = %v", errs)
	}
	for i, result := range results {
		text, err := result.Response.Text()
		if err != nil {
			t.Fatal(err)
		}
		want := []string{"/one", "/two"}[i]
		if text != want {
			t.Fatalf("result %d body = %q, want %q", i, text, want)
		}
	}

	// A batch with a failure surfaces it through both helpers.
	bad := c.GetRestHandler().BatchGet(context.Background(), []string{"http://127.0.0.1:1/dead"}, nil)
	defer requester.CloseResults(bad)
	if requester.FirstError(bad) == nil {
		t.Fatal("expected requester.FirstError to report the dead host")
	}
	if len(requester.Errors(bad)) != 1 {
		t.Fatalf("Errors = %v", requester.Errors(bad))
	}
}

func TestTLSVerificationOnByDefault(t *testing.T) {
	// A zero-value requester.ConnSettings must still verify certificates: this is the
	// struct-literal path that used to silently disable verification.
	zero, err := requester.NewConnection(1, requester.WithSettings(&requester.ConnSettings{}))
	if err != nil {
		t.Fatal(err)
	}
	if zero.GetTransport().TLSClientConfig.InsecureSkipVerify {
		t.Fatal("a zero-value requester.ConnSettings disabled TLS verification")
	}

	defaults, err := requester.NewConnection(2)
	if err != nil {
		t.Fatal(err)
	}
	if defaults.GetTransport().TLSClientConfig.InsecureSkipVerify {
		t.Fatal("the default settings disabled TLS verification")
	}

	// It really rejects a self-signed certificate.
	tlsServer := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	defer tlsServer.Close()
	// The rejected handshake below is the point of the test, so keep the
	// server from logging it as if something went wrong.
	tlsServer.Config.ErrorLog = log.New(io.Discard, "", 0)

	if _, err := zero.GetRestHandler().Get(context.Background(), tlsServer.URL, nil); err == nil {
		t.Fatal("expected the self-signed certificate to be rejected")
	}

	// Opting out explicitly works.
	insecure, err := requester.NewConnection(3, requester.WithInsecureTLS())
	if err != nil {
		t.Fatal(err)
	}
	resp, err := insecure.GetRestHandler().Get(context.Background(), tlsServer.URL, nil)
	if err != nil {
		t.Fatalf("SkipPeerVerification did not take effect: %v", err)
	}
	resp.Close()
}

// TestDiscardReturnsLargeBodyConnection checks Discard drains past the 64 KiB
// that Close stops at, so a large unread body still keeps its connection.
func TestDiscardReturnsLargeBodyConnection(t *testing.T) {
	server, conns := serveCountingConns(t, func(w http.ResponseWriter, r *http.Request) {
		w.Write(bytes.Repeat([]byte("z"), 1<<20))
	})

	c := newTestConn(t, false, nil)
	for range 3 {
		resp, err := c.GetRestHandler().Get(context.Background(), server.URL, nil)
		if err != nil {
			t.Fatal(err)
		}
		if err := resp.Discard(); err != nil {
			t.Fatal(err)
		}
	}
	if n := conns.Load(); n != 1 {
		t.Fatalf("opened %d connections for 3 discarded 1 MiB bodies, want 1", n)
	}
}

// TestDiscardSkipsBodyOverLimit checks a body declared larger than
// MaxResponseBodySize is not read at all: the connection is dropped instead.
func TestDiscardSkipsBodyOverLimit(t *testing.T) {
	server, conns := serveCountingConns(t, func(w http.ResponseWriter, r *http.Request) {
		w.Write(bytes.Repeat([]byte("z"), 256<<10))
	})

	c := newTestConn(t, false, func(s *requester.ConnSettings) { s.SetMaxResponseBodySize(1024) })
	for range 2 {
		resp, err := c.GetRestHandler().Get(context.Background(), server.URL, nil)
		if err != nil {
			t.Fatal(err)
		}
		if err := resp.Discard(); err != nil {
			t.Fatal(err)
		}
	}
	if n := conns.Load(); n != 2 {
		t.Fatalf("opened %d connections, want 2: an over-limit body should not be drained", n)
	}
}

// TestBytesWithDeclaredLength covers the Content-Length fast path: the exact
// body comes back, and a body cut short of its declared length is an error.
func TestBytesWithDeclaredLength(t *testing.T) {
	want := strings.Repeat("0123456789", 10_000)
	server := serve(t, func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("truncate") != "" {
			w.Header().Set("Content-Length", "100")
			w.Write([]byte("only fifty bytes.................................."))
			return
		}
		w.Header().Set("Content-Length", strconv.Itoa(len(want)))
		io.WriteString(w, want)
	})
	c := newTestConn(t, false, nil)

	resp, err := c.GetRestHandler().Get(context.Background(), server.URL, nil)
	if err != nil {
		t.Fatal(err)
	}
	if got, err := resp.Text(); err != nil || got != want {
		t.Fatalf("len = %d, err = %v", len(got), err)
	}

	resp, err = c.GetRestHandler().Get(context.Background(), server.URL+"?truncate=1", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Close()
	if _, err := resp.Bytes(); !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatalf("truncated body: err = %v, want io.ErrUnexpectedEOF", err)
	}
}
