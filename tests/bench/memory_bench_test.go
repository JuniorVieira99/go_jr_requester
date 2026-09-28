package bench

import (
	"context"
	"fmt"
	requester "github.com/JuniorVieira99/go_jr_requester/jr_requester"
	"io"
	"net/http"
	"runtime"
	"strings"
	"testing"
	"time"
)

// BenchmarkConnectionFootprint reports the retained heap and goroutines of a
// connection holding one pooled keep-alive connection. The server runs in the
// same process, so its half of each TCP connection is counted too; the
// net/http case carries the same server cost, and the difference between the
// two is what the library adds. It dials n connections per iteration, so
// -short skips it.
func BenchmarkConnectionFootprint(b *testing.B) {
	server := newServer(b)
	url := sizedURL(server, 0)
	const n = 25
	skipDialHeavy(b)

	b.Run("jr_requester", func(b *testing.B) {
		var heapPer, goroutinesPer float64
		for b.Loop() {
			before, beforeG := heapAlloc(), runtime.NumGoroutine()
			conns := make([]*requester.Connection, n)
			for i := range conns {
				conn, err := requester.NewConnection(uint64(i), requester.WithKeepAlive(1, 1))
				if err != nil {
					b.Fatal(err)
				}
				resp, err := conn.GetRestHandler().Get(context.Background(), url, nil)
				if err != nil {
					b.Fatal(err)
				}
				resp.Close()
				conns[i] = conn
			}
			heapPer = float64(int64(heapAlloc())-int64(before)) / n
			goroutinesPer = float64(runtime.NumGoroutine()-beforeG) / n
			for _, conn := range conns {
				conn.Shutdown()
			}
			server.CloseClientConnections()
		}
		b.ReportMetric(heapPer, "heap-B/conn")
		b.ReportMetric(goroutinesPer, "goroutines/conn")
	})

	b.Run("net/http", func(b *testing.B) {
		var heapPer, goroutinesPer float64
		for b.Loop() {
			before, beforeG := heapAlloc(), runtime.NumGoroutine()
			transports := make([]*http.Transport, n)
			for i := range transports {
				transport := http.DefaultTransport.(*http.Transport).Clone()
				transport.MaxIdleConnsPerHost, transport.MaxConnsPerHost = 1, 1
				resp, err := (&http.Client{Transport: transport}).Get(url)
				if err != nil {
					b.Fatal(err)
				}
				io.Copy(io.Discard, resp.Body)
				resp.Body.Close()
				transports[i] = transport
			}
			heapPer = float64(int64(heapAlloc())-int64(before)) / n
			goroutinesPer = float64(runtime.NumGoroutine()-beforeG) / n
			for _, transport := range transports {
				transport.CloseIdleConnections()
			}
			server.CloseClientConnections()
		}
		b.ReportMetric(heapPer, "heap-B/conn")
		b.ReportMetric(goroutinesPer, "goroutines/conn")
	})
}

// BenchmarkSustainedLoad runs a fixed burst of concurrent requests per
// iteration and reports how much the live heap grew across the whole run. A
// leak shows up as growth that scales with -benchtime rather than levelling
// off.
func BenchmarkSustainedLoad(b *testing.B) {
	server := newServer(b)
	url := sizedURL(server, 4<<10)
	const burst = 64

	urls := make([]string, burst)
	for i := range urls {
		urls[i] = url
	}
	batch := newConn(b, requester.WithAsync(), requester.WithKeepAlive(burst, burst))

	// Warm the pool so its connections are not counted as growth.
	requester.CloseResults(batch.GetRestHandler().BatchGet(context.Background(), urls, nil))
	before := heapAlloc()

	b.ReportAllocs()
	for b.Loop() {
		results := batch.GetRestHandler().BatchGet(context.Background(), urls, nil)
		if err := requester.FirstError(results); err != nil {
			b.Fatal(err)
		}
		for _, r := range results {
			if _, err := r.Response.Bytes(); err != nil {
				b.Fatal(err)
			}
		}
	}

	growth := float64(int64(heapAlloc()) - int64(before))
	b.ReportMetric(growth/1024, "heap-growth-KiB")
	b.ReportMetric(float64(batch.InFlight()), "inflight-after")
}

// TestNoGoroutineLeak checks a connection that has served requests, including
// ones whose body was never read, gives back every goroutine once it is shut
// down. It runs with the ordinary test suite, not only under -bench.
func TestNoGoroutineLeak(t *testing.T) {
	baseline := runtime.NumGoroutine()

	server := newServer(t)
	const n = 32
	conn := newConn(t, requester.WithAsync(), requester.WithMetrics(), requester.WithKeepAlive(n, n))
	rest := conn.GetRestHandler()
	ctx := context.Background()

	urls := make([]string, n)
	for i := range urls {
		urls[i] = sizedURL(server, 128<<10)
	}
	for round := range 5 {
		results := rest.BatchGet(ctx, urls, nil)
		if err := requester.FirstError(results); err != nil {
			t.Fatal(err)
		}
		// Half read, half closed unread: the second kind drops its connection.
		for i, r := range results {
			if (i+round)%2 == 0 {
				if _, err := r.Response.Bytes(); err != nil {
					t.Fatal(err)
				}
			}
			r.Response.Close()
		}
	}
	if n := conn.InFlight(); n != 0 {
		t.Fatalf("InFlight = %d after every body was closed, want 0", n)
	}

	conn.Shutdown()
	server.Close()

	deadline := time.Now().Add(5 * time.Second)
	for runtime.NumGoroutine() > baseline && time.Now().Before(deadline) {
		time.Sleep(20 * time.Millisecond)
	}
	if n := runtime.NumGoroutine(); n > baseline {
		buf := make([]byte, 1<<16)
		t.Fatalf("%d goroutines outlived the connection:\n%s", n-baseline, buf[:runtime.Stack(buf, true)])
	}
}

// answerLocally short-circuits every request with an empty response, so a
// benchmark can hold requests in flight without dialling. That matters for
// setup repeated every iteration: real dials would run the machine out of
// ephemeral ports long before b.Loop is done.
func answerLocally(req *http.Request, next requester.RoundTripFunc) (*http.Response, error) {
	return &http.Response{
		StatusCode: http.StatusOK,
		Header:     make(http.Header),
		Body:       io.NopCloser(strings.NewReader("")),
		Request:    req,
	}, nil
}

// BenchmarkShutdown measures the lifecycle of a connection with open requests
// in flight: build it, open them, shut it down. ns/op covers the whole cycle;
// shutdown-ns is Shutdown alone, which walks and cancels the inflight map.
// Shutdown is timed by hand because StopTimer/StartTimer stop the world to
// read memory stats and would take minutes around a call this short.
func BenchmarkShutdown(b *testing.B) {
	const url = "http://127.0.0.1/never-dialled"

	for _, open := range []int{0, 32} {
		b.Run(fmt.Sprintf("open=%d", open), func(b *testing.B) {
			var spent time.Duration
			b.ReportAllocs()
			for b.Loop() {
				conn, err := requester.NewConnection(1, requester.WithMiddleware(answerLocally))
				if err != nil {
					b.Fatal(err)
				}
				// Unclosed bodies stay registered as in flight.
				held := make([]*requester.Response, open)
				for i := range held {
					if held[i], err = conn.GetRestHandler().Get(context.Background(), url, nil); err != nil {
						b.Fatal(err)
					}
				}

				start := time.Now()
				conn.Shutdown()
				spent += time.Since(start)

				for _, r := range held {
					r.Close()
				}
			}
			b.ReportMetric(float64(spent.Nanoseconds())/float64(b.N), "shutdown-ns")
		})
	}
}
