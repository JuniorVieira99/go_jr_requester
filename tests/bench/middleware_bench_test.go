package bench

import (
	"context"
	"fmt"
	requester "github.com/JuniorVieira99/go_jr_requester/jr_requester"
	"log/slog"
	"net/http"
	"testing"
	"time"
)

// passThrough is the cheapest possible middleware, so a deep stack of them
// isolates the cost of the chain itself.
func passThrough(req *http.Request, next requester.RoundTripFunc) (*http.Response, error) {
	return next(req)
}

// BenchmarkMiddlewareDepth measures how the per-request cost grows with the
// number of registered middleware.
func BenchmarkMiddlewareDepth(b *testing.B) {
	server := newServer(b)
	url := sizedURL(server, 0)

	for _, depth := range []int{0, 1, 4, 16} {
		b.Run(fmt.Sprintf("depth=%d", depth), func(b *testing.B) {
			mws := make([]requester.Middleware, depth)
			for i := range mws {
				mws[i] = passThrough
			}
			conn := newConn(b, requester.WithMiddleware(mws...))
			rest := conn.GetRestHandler()
			ctx := context.Background()

			b.ReportAllocs()
			for b.Loop() {
				resp, err := rest.Get(ctx, url, nil)
				if err != nil {
					b.Fatal(err)
				}
				resp.Close()
			}
		})
	}
}

// BenchmarkMiddlewareRecipes measures a realistic stack of the built-in
// recipes against no middleware at all.
func BenchmarkMiddlewareRecipes(b *testing.B) {
	server := newServer(b)
	url := sizedURL(server, 0)
	token := func(context.Context) (string, error) { return "token", nil }

	stacks := map[string][]requester.Middleware{
		"none": nil,
		"guards": {
			requester.MiddlewareAllowedHosts("127.0.0.1"),
			requester.MiddlewareSecureHeaders([]string{"X-Tenant"}),
			requester.MiddlewareSecureQueryParamsAndValues(map[string][]string{"size": {"0"}}),
		},
		"decorators": {
			requester.MiddlewareSetHeaders(map[string]string{"User-Agent": "bench/1"}),
			requester.MiddlewareBearerToken(token),
			requester.MiddlewareRequestID(""),
			requester.MiddlewareTimeout(10 * time.Second),
		},
		"logger": {
			requester.MiddlewareLogger(slog.New(slog.DiscardHandler)),
		},
	}
	for _, name := range []string{"none", "guards", "decorators", "logger"} {
		b.Run(name, func(b *testing.B) {
			conn := newConn(b, requester.WithMiddleware(stacks[name]...))
			rest := conn.GetRestHandler()
			ctx := context.Background()
			header := map[string]string{"X-Tenant": "acme"}

			b.ReportAllocs()
			for b.Loop() {
				resp, err := rest.Get(ctx, url, header)
				if err != nil {
					b.Fatal(err)
				}
				resp.Close()
			}
		})
	}
}

// BenchmarkMiddlewareStackEdit measures Add and Remove on a live stack, which
// copy the entry slice on every change.
func BenchmarkMiddlewareStackEdit(b *testing.B) {
	for _, size := range []int{0, 16} {
		b.Run(fmt.Sprintf("existing=%d", size), func(b *testing.B) {
			stack := requester.NewMiddlewareStack()
			for range size {
				stack.Add(passThrough)
			}

			b.ReportAllocs()
			for b.Loop() {
				id, err := stack.Add(passThrough)
				if err != nil {
					b.Fatal(err)
				}
				if err := stack.Remove(id); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
