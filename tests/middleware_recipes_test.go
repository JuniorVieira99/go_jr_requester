package tests

import (
	"bytes"
	"context"
	"errors"
	"io"
	requester "jr_requester/jr_requester"
	"log/slog"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// recipeConn builds a connection running the given middleware.
func recipeConn(t *testing.T, mw ...requester.Middleware) *requester.Connection {
	t.Helper()
	conn, err := requester.NewConnection(1, requester.WithMiddleware(mw...))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(conn.Shutdown)
	return conn
}

// trackedBody records whether the transport layer closed the request body.
type trackedBody struct {
	io.Reader
	closed atomic.Bool
}

func (b *trackedBody) Close() error {
	b.closed.Store(true)
	return nil
}

// TestRecipeErrorsWrapSentinels checks every rejection can be matched with
// errors.Is, including the ones that carry a detailed message.
func TestRecipeErrorsWrapSentinels(t *testing.T) {
	server := serve(t, func(w http.ResponseWriter, r *http.Request) {})

	cases := []struct {
		name string
		mw   requester.Middleware
		url  string
		want error
	}{
		{"header missing", requester.MiddlewareSecureHeaders([]string{"X-Tenant"}), server.URL, requester.ErrMissingHeader},
		{"header value missing", requester.MiddlewareSecureHeadersAndValues(map[string][]string{"X-Tenant": {"acme"}}), server.URL, requester.ErrMissingHeader},
		{"query missing", requester.MiddlewareSecureQueryParams([]string{"page"}), server.URL, requester.ErrMissingQueryParam},
		{"query value missing", requester.MiddlewareSecureQueryParamsAndValues(map[string][]string{"page": {"1"}}), server.URL + "?page=2", requester.ErrMissingQueryParam},
		{"token missing", requester.MiddlewareSecureToken("X-Token", "secret"), server.URL, requester.ErrAuthorizationHeaderMissing},
		{"bad config", requester.MiddlewareSecureHeadersAndValues(map[string][]string{"X-Tenant": {""}}), server.URL, requester.ErrInvalidRecipe},
		{"host", requester.MiddlewareAllowedHosts("example.com"), server.URL, requester.ErrHostNotAllowed},
		{"https", requester.MiddlewareRequireHTTPS(), server.URL, requester.ErrInsecureScheme},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			conn := recipeConn(t, tc.mw)
			_, err := conn.GetRestHandler().Get(context.Background(), tc.url, nil)
			if !errors.Is(err, tc.want) {
				t.Fatalf("err = %v, want %v", err, tc.want)
			}
		})
	}
}

// TestRecipeHeaderNamesAreCaseInsensitive checks a lower-case key in the
// expected map still matches the canonical header on the request.
func TestRecipeHeaderNamesAreCaseInsensitive(t *testing.T) {
	server := serve(t, func(w http.ResponseWriter, r *http.Request) {})
	conn := recipeConn(t, requester.MiddlewareSecureHeadersAndValues(map[string][]string{
		"x-tenant": {"acme"},
	}))

	resp, err := conn.GetRestHandler().Get(context.Background(), server.URL, map[string]string{"X-Tenant": "acme"})
	if err != nil {
		t.Fatal(err)
	}
	resp.Close()
}

// TestRecipeRejectClosesRequestBody checks a refused request still has its
// body closed, as the RoundTripper contract requires.
func TestRecipeRejectClosesRequestBody(t *testing.T) {
	server := serve(t, func(w http.ResponseWriter, r *http.Request) {})
	conn := recipeConn(t, requester.MiddlewareSecureHeaders([]string{"X-Tenant"}))

	body := &trackedBody{Reader: strings.NewReader("payload")}
	req, err := http.NewRequestWithContext(context.Background(), http.MethodPost, server.URL, body)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := conn.DoRequest(context.Background(), req); err == nil {
		t.Fatal("expected the request to be rejected")
	}
	if !body.closed.Load() {
		t.Fatal("the rejected request's body was not closed")
	}
}

// TestRecipeConfigIsCopied checks editing the caller's map after building the
// middleware does not change the rule.
func TestRecipeConfigIsCopied(t *testing.T) {
	server := serve(t, func(w http.ResponseWriter, r *http.Request) {})
	rules := map[string][]string{"X-Tenant": {"acme"}}
	conn := recipeConn(t, requester.MiddlewareSecureHeadersAndValues(rules))
	rules["X-Tenant"][0] = "other"

	resp, err := conn.GetRestHandler().Get(context.Background(), server.URL, map[string]string{"X-Tenant": "acme"})
	if err != nil {
		t.Fatal(err)
	}
	resp.Close()
}

// TestRecipeDecoratorsReachServer checks the header-setting recipes land on
// the wire, and that a request id the caller chose is kept.
func TestRecipeDecoratorsReachServer(t *testing.T) {
	seen := make(chan http.Header, 2)
	server := serve(t, func(w http.ResponseWriter, r *http.Request) { seen <- r.Header.Clone() })

	conn := recipeConn(t,
		requester.MiddlewareSetHeaders(map[string]string{"User-Agent": "jr/1"}),
		requester.MiddlewareBearerToken(func(context.Context) (string, error) { return "tok", nil }),
		requester.MiddlewareRequestID(""),
	)

	resp, err := conn.GetRestHandler().Get(context.Background(), server.URL, nil)
	if err != nil {
		t.Fatal(err)
	}
	resp.Close()
	h := <-seen
	if h.Get("User-Agent") != "jr/1" || h.Get("Authorization") != "Bearer tok" || len(h.Get("X-Request-Id")) != 32 {
		t.Fatalf("headers = %v", h)
	}

	resp, err = conn.GetRestHandler().Get(context.Background(), server.URL, map[string]string{"X-Request-Id": "mine"})
	if err != nil {
		t.Fatal(err)
	}
	resp.Close()
	if got := (<-seen).Get("X-Request-Id"); got != "mine" {
		t.Fatalf("X-Request-Id = %q, want the caller's", got)
	}
}

// TestRecipeTimeoutKeepsBodyReadable checks the deadline is not cancelled when
// the middleware returns, which would break reading the body, but does fire on
// a slow server.
func TestRecipeTimeoutKeepsBodyReadable(t *testing.T) {
	server := serve(t, func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("slow") != "" {
			time.Sleep(300 * time.Millisecond)
		}
		w.Write(bytes.Repeat([]byte("x"), 64<<10))
	})
	conn := recipeConn(t, requester.MiddlewareTimeout(100*time.Millisecond))

	resp, err := conn.GetRestHandler().Get(context.Background(), server.URL, nil)
	if err != nil {
		t.Fatal(err)
	}
	body, err := resp.Text()
	resp.Close()
	if err != nil || len(body) != 64<<10 {
		t.Fatalf("len(body) = %d, err = %v", len(body), err)
	}

	if _, err := conn.GetRestHandler().Get(context.Background(), server.URL+"?slow=1", nil); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("err = %v, want DeadlineExceeded", err)
	}
}

// TestRecipeLoggerOmitsHeaders checks the logger records the request but not
// its headers.
func TestRecipeLoggerOmitsHeaders(t *testing.T) {
	server := serve(t, func(w http.ResponseWriter, r *http.Request) {})
	var buf bytes.Buffer
	conn := recipeConn(t, requester.MiddlewareLogger(slog.New(slog.NewTextHandler(&buf, nil))))

	resp, err := conn.GetRestHandler().Get(context.Background(), server.URL, map[string]string{"Authorization": "secret"})
	if err != nil {
		t.Fatal(err)
	}
	resp.Close()
	if out := buf.String(); !strings.Contains(out, "status=200") || strings.Contains(out, "secret") {
		t.Fatalf("log = %q", out)
	}
}
