package bench

import (
	"bytes"
	"context"
	"io"
	requester "jr_requester/jr_requester"
	"net/http"
	"testing"
	"time"
)

// BenchmarkNewConnection measures building and tearing down a connection,
// which matters for code that makes one per task instead of sharing it.
func BenchmarkNewConnection(b *testing.B) {
	cases := []struct {
		name string
		opts []requester.ConnOption
	}{
		{"defaults", nil},
		{"configured", []requester.ConnOption{
			requester.WithKeepAlive(16, 32),
			requester.WithRetries(3, time.Millisecond),
			requester.WithApiKey("X-API-Key", "key"),
			requester.WithHeaders(map[string]string{"Accept": "application/json"}),
			requester.WithMetrics(),
			requester.WithCookies(),
		}},
	}
	for _, tc := range cases {
		b.Run(tc.name, func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				conn, err := requester.NewConnection(1, tc.opts...)
				if err != nil {
					b.Fatal(err)
				}
				conn.Shutdown()
			}
		})
	}
}

// BenchmarkGet measures one sequential GET on a warm pool, read fully and
// closed, across response sizes. Compare it with BenchmarkBaselineGet for the
// library's overhead over plain net/http.
func BenchmarkGet(b *testing.B) {
	server := newServer(b)
	for _, size := range bodySizes {
		b.Run(sizeName(size), func(b *testing.B) {
			conn := newConn(b)
			rest := conn.GetRestHandler()
			url := sizedURL(server, size)
			ctx := context.Background()

			b.ReportAllocs()
			b.SetBytes(int64(size))
			for b.Loop() {
				resp, err := rest.Get(ctx, url, nil)
				if err != nil {
					b.Fatal(err)
				}
				if _, err := resp.Bytes(); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// BenchmarkBaselineGet is BenchmarkGet with a bare net/http client.
func BenchmarkBaselineGet(b *testing.B) {
	server := newServer(b)
	for _, size := range bodySizes {
		b.Run(sizeName(size), func(b *testing.B) {
			client := newBaselineClient(b)
			url := sizedURL(server, size)
			ctx := context.Background()

			b.ReportAllocs()
			b.SetBytes(int64(size))
			for b.Loop() {
				req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
				if err != nil {
					b.Fatal(err)
				}
				resp, err := client.Do(req)
				if err != nil {
					b.Fatal(err)
				}
				if _, err := io.ReadAll(resp.Body); err != nil {
					b.Fatal(err)
				}
				resp.Body.Close()
			}
		})
	}
}

// BenchmarkPost measures sending a request body of each size.
func BenchmarkPost(b *testing.B) {
	server := newServer(b)
	for _, size := range bodySizes {
		b.Run(sizeName(size), func(b *testing.B) {
			conn := newConn(b)
			rest := conn.GetRestHandler()
			url := sizedURL(server, 0)
			ctx := context.Background()
			body := payload[:size]

			b.ReportAllocs()
			b.SetBytes(int64(size))
			for b.Loop() {
				resp, err := rest.Post(ctx, url, nil, bytes.NewReader(body))
				if err != nil {
					b.Fatal(err)
				}
				resp.Close()
			}
		})
	}
}

// BenchmarkGetParallel measures throughput with one request in flight per
// goroutine, which is where lock contention in the connection would show.
func BenchmarkGetParallel(b *testing.B) {
	server := newServer(b)
	for _, size := range []int{0, 64 << 10} {
		b.Run(sizeName(size), func(b *testing.B) {
			conn := newConn(b)
			rest := conn.GetRestHandler()
			url := sizedURL(server, size)

			b.ReportAllocs()
			b.SetBytes(int64(size))
			b.RunParallel(func(pb *testing.PB) {
				ctx := context.Background()
				for pb.Next() {
					resp, err := rest.Get(ctx, url, nil)
					if err != nil {
						b.Error(err)
						return
					}
					if _, err := resp.Bytes(); err != nil {
						b.Error(err)
						return
					}
				}
			})
		})
	}
}

// BenchmarkKeepAlive shows what pooling is worth. Without it every request
// dials a fresh TCP connection, which is the library's default.
//
// The "off" case dials per request, so -short skips it.
func BenchmarkKeepAlive(b *testing.B) {
	server := newServer(b)
	url := sizedURL(server, 1<<10)

	run := func(b *testing.B, conn *requester.Connection) {
		rest := conn.GetRestHandler()
		ctx := context.Background()
		b.ReportAllocs()
		for b.Loop() {
			resp, err := rest.Get(ctx, url, nil)
			if err != nil {
				b.Fatal(err)
			}
			if _, err := resp.Bytes(); err != nil {
				b.Fatal(err)
			}
		}
	}

	b.Run("on", func(b *testing.B) {
		run(b, newConn(b))
	})
	b.Run("off", func(b *testing.B) {
		skipDialHeavy(b)
		conn, err := requester.NewConnection(1)
		if err != nil {
			b.Fatal(err)
		}
		b.Cleanup(conn.Shutdown)
		run(b, conn)
	})
}

// BenchmarkFeatures measures the per-request cost of the optional layers, each
// switched on alone, against a connection with none of them.
func BenchmarkFeatures(b *testing.B) {
	server := newServer(b)
	url := sizedURL(server, 1<<10)

	cases := []struct {
		name string
		opts []requester.ConnOption
	}{
		{"none", nil},
		{"metrics", []requester.ConnOption{requester.WithMetrics()}},
		{"retries", []requester.ConnOption{requester.WithRetries(3, time.Millisecond)}},
		{"headers+apikey", []requester.ConnOption{
			requester.WithApiKey("X-API-Key", "key"),
			requester.WithHeaders(map[string]string{"Accept": "application/json", "User-Agent": "bench/1"}),
		}},
		{"cookies", []requester.ConnOption{requester.WithCookies()}},
		{"compression-manual", []requester.ConnOption{requester.WithCompressionMode(requester.CompressionManual)}},
	}
	for _, tc := range cases {
		b.Run(tc.name, func(b *testing.B) {
			conn := newConn(b, tc.opts...)
			rest := conn.GetRestHandler()
			ctx := context.Background()

			b.ReportAllocs()
			for b.Loop() {
				resp, err := rest.Get(ctx, url, nil)
				if err != nil {
					b.Fatal(err)
				}
				if _, err := resp.Bytes(); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// BenchmarkDoRequest measures the lowest-level entry point, which skips
// building the request and wrapping the response.
func BenchmarkDoRequest(b *testing.B) {
	server := newServer(b)
	conn := newConn(b)
	ctx := context.Background()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, sizedURL(server, 1<<10), nil)
	if err != nil {
		b.Fatal(err)
	}

	b.ReportAllocs()
	for b.Loop() {
		resp, err := conn.DoRequest(ctx, req)
		if err != nil {
			b.Fatal(err)
		}
		io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
	}
}
