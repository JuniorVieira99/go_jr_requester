package bench

import (
	"bytes"
	"context"
	"fmt"
	requester "github.com/JuniorVieira99/go_jr_requester/jr_requester"
	"testing"
)

// BenchmarkBatchGet measures a whole batch per iteration, sequential against
// concurrent, so ns/op is the wall time of one batch.
//
// A batch holds every response open until it returns, so its pool must allow
// at least as many connections as it has requests; with fewer, the surplus
// requests wait for a connection that is never freed and stall until Timeout.
func BenchmarkBatchGet(b *testing.B) {
	server := newServer(b)
	for _, n := range []int{10, 100} {
		urls := make([]string, n)
		for i := range urls {
			urls[i] = sizedURL(server, 1<<10)
		}
		for _, mode := range []string{"sync", "async"} {
			b.Run(fmt.Sprintf("%s/n=%d", mode, n), func(b *testing.B) {
				opts := []requester.ConnOption{requester.WithKeepAlive(uint64(n), uint64(n))}
				if mode == "async" {
					opts = append(opts, requester.WithAsync())
				}
				conn := newConn(b, opts...)
				rest := conn.GetRestHandler()
				ctx := context.Background()

				b.ReportAllocs()
				for b.Loop() {
					results := rest.BatchGet(ctx, urls, nil)
					if err := requester.FirstError(results); err != nil {
						b.Fatal(err)
					}
					for _, r := range results {
						if _, err := r.Response.Bytes(); err != nil {
							b.Fatal(err)
						}
					}
				}
				b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N*n), "ns/request")
			})
		}
	}
}

// BenchmarkBatchSharedBody measures DoManualBatchRequests, which buffers one
// body and replays it to every URL.
func BenchmarkBatchSharedBody(b *testing.B) {
	server := newServer(b)
	const n = 20
	urls := make([]string, n)
	for i := range urls {
		urls[i] = sizedURL(server, 0)
	}
	body := payload[:64<<10]

	conn := newConn(b, requester.WithAsync(), requester.WithKeepAlive(n, n))
	ctx := context.Background()

	b.ReportAllocs()
	b.SetBytes(int64(len(body) * n))
	for b.Loop() {
		results := conn.DoManualBatchRequests(ctx, requester.POST, urls, nil, bytes.NewReader(body))
		if err := requester.FirstError(results); err != nil {
			b.Fatal(err)
		}
		requester.CloseResults(results)
	}
}
