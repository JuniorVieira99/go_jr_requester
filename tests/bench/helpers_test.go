package bench

import (
	"bytes"
	"fmt"
	"io"
	requester "jr_requester/jr_requester"
	"net/http"
	"net/http/httptest"
	"runtime"
	"strconv"
	"testing"
	"time"
)

// Benchmarks for the Connection struct. They live outside the library package,
// like the rest of the suite, so they measure only what a consumer can call.
//
// Every benchmark runs against a local httptest server, so the numbers are the
// client's overhead plus loopback, never the network. Compare runs with
// benchstat rather than reading single results:
//
//	go test ./tests/bench -run '^$' -bench . -benchmem -count 10 > new.txt
//	benchstat old.txt new.txt

// bodySizes are the response and request sizes most benchmarks sweep.
var bodySizes = []int{0, 1 << 10, 64 << 10, 1 << 20}

// payload is shared, read-only filler for response and request bodies.
var payload = bytes.Repeat([]byte("x"), 1<<20)

// sizeName renders a byte count as a sub-benchmark name.
func sizeName(n int) string {
	switch {
	case n >= 1<<20:
		return fmt.Sprintf("%dMiB", n>>20)
	case n >= 1<<10:
		return fmt.Sprintf("%dKiB", n>>10)
	default:
		return fmt.Sprintf("%dB", n)
	}
}

// newServer starts a server that answers with ?size=N bytes of filler and
// drains any request body. It is closed when the benchmark ends.
func newServer(tb testing.TB) *httptest.Server {
	tb.Helper()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Body != nil {
			io.Copy(io.Discard, r.Body)
		}
		size, _ := strconv.Atoi(r.URL.Query().Get("size"))
		size = min(max(size, 0), len(payload))
		w.Header().Set("Content-Length", strconv.Itoa(size))
		w.Write(payload[:size])
	}))
	tb.Cleanup(server.Close)
	return server
}

// sizedURL points at the server's filler endpoint.
func sizedURL(server *httptest.Server, size int) string {
	return fmt.Sprintf("%s/?size=%d", server.URL, size)
}

// poolSize is enough pooled connections that parallel benchmarks measure the
// client, not the wait for a free connection.
func poolSize() uint64 {
	return uint64(runtime.GOMAXPROCS(0) * 4)
}

// newConn builds a pooled connection with opts applied on top, shut down when
// the benchmark ends.
func newConn(tb testing.TB, opts ...requester.ConnOption) *requester.Connection {
	tb.Helper()
	all := append([]requester.ConnOption{requester.WithKeepAlive(poolSize(), poolSize())}, opts...)
	conn, err := requester.NewConnection(1, all...)
	if err != nil {
		tb.Fatalf("NewConnection: %v", err)
	}
	tb.Cleanup(conn.Shutdown)
	return conn
}

// newBaselineClient is a plain net/http client pooled the same way as newConn,
// the yardstick for how much the library adds on top.
func newBaselineClient(tb testing.TB) *http.Client {
	tb.Helper()
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.MaxIdleConns = int(poolSize())
	transport.MaxIdleConnsPerHost = int(poolSize())
	transport.MaxConnsPerHost = int(poolSize())
	tb.Cleanup(transport.CloseIdleConnections)
	return &http.Client{Transport: transport, Timeout: 30 * time.Second}
}

// skipDialHeavy skips a benchmark that opens a new TCP connection per
// iteration. Each one leaves a socket in TIME_WAIT for minutes, and on Windows
// a few repeated runs exhaust the ephemeral ports and fail every benchmark
// after them, so -short leaves these out for -count runs.
func skipDialHeavy(b *testing.B) {
	b.Helper()
	if testing.Short() {
		b.Skip("dials once per iteration; skipped under -short to spare ephemeral ports")
	}
}

// heapAlloc settles the garbage collector and reports the live heap.
func heapAlloc() uint64 {
	runtime.GC()
	runtime.GC()
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	return m.HeapAlloc
}
