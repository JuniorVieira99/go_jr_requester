package tests

import (
	"context"
	"encoding/json"
	"errors"
	requester "github.com/JuniorVieira99/go_jr_requester/jr_requester"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

// TestAsyncBatchCompletes guards the deadlock: the async batch goroutines used
// to skip wg.Done(), so wg.Wait() never returned.
func TestAsyncBatchCompletes(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	defer server.Close()

	c := newTestConn(t, false, func(s *requester.ConnSettings) { s.SetUseAsync(true) })
	urls := make([]string, 12)
	for i := range urls {
		urls[i] = server.URL
	}

	done := make(chan []requester.BatchResult, 1)
	go func() { done <- c.GetRestHandler().BatchGet(context.Background(), urls, nil) }()

	select {
	case results := <-done:
		defer closeBodies(results)
		if len(results) != len(urls) {
			t.Fatalf("len(results) = %d, want %d", len(results), len(urls))
		}
		for i, r := range results {
			if r.Err != nil {
				t.Fatalf("result %d: %v", i, r.Err)
			}
			if r.Request == nil {
				t.Fatalf("result %d has no Request", i)
			}
		}
	case <-time.After(15 * time.Second):
		t.Fatal("async batch deadlocked")
	}
}

// TestAsyncBatchBodiesReadable guards the batch context: it used to be
// cancelled when the batch returned, so every body too large to have been
// buffered already failed with context.Canceled when the caller read it.
func TestAsyncBatchBodiesReadable(t *testing.T) {
	const size = 1 << 20
	body := strings.Repeat("x", size)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		io.WriteString(w, body)
	}))
	defer server.Close()

	c := newTestConn(t, false, func(s *requester.ConnSettings) { s.SetUseAsync(true) })
	defer c.Shutdown()
	urls := []string{server.URL, server.URL, server.URL}

	results := c.GetRestHandler().BatchGet(context.Background(), urls, nil)
	defer closeBodies(results)
	for i, r := range results {
		if r.Err != nil {
			t.Fatalf("result %d: %v", i, r.Err)
		}
		got, err := r.Response.Bytes()
		if err != nil || len(got) != size {
			t.Fatalf("result %d: read %d bytes, err = %v", i, len(got), err)
		}
	}
	if n := c.InFlight(); n != 0 {
		t.Fatalf("InFlight = %d after every body was read, want 0", n)
	}
}

// TestBatchResultsStayOrdered checks that every entry is populated, in order,
// including the ones that never produced a response.
func TestBatchResultsStayOrdered(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(r.URL.Path))
	}))
	defer server.Close()

	for _, async := range []bool{false, true} {
		c := newTestConn(t, false, func(s *requester.ConnSettings) { s.SetUseAsync(async) })
		urls := []string{server.URL + "/a", "://not a url", server.URL + "/c"}

		results := c.GetRestHandler().BatchGet(context.Background(), urls, nil)
		closeBodies(results)

		if len(results) != 3 {
			t.Fatalf("async=%v: len = %d", async, len(results))
		}
		if results[0].Err != nil || results[2].Err != nil {
			t.Fatalf("async=%v: good requests failed: %v %v", async, results[0].Err, results[2].Err)
		}
		if results[1].Err == nil {
			t.Fatalf("async=%v: malformed url should have failed", async)
		}
		// The build failure must name the URL, not just say "nil request".
		if strings.Contains(results[1].Err.Error(), "nil request") {
			t.Fatalf("async=%v: build error was masked: %v", async, results[1].Err)
		}
	}
}

// TestFailFastMarksSkipped checks the sequential path labels the requests it
// abandoned instead of leaving them as zero-valued successes.
func TestFailFastMarksSkipped(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	defer server.Close()

	c := newTestConn(t, false, func(s *requester.ConnSettings) {
		s.SetUseAsync(false)
		s.SetUseFailFast(true)
	})
	urls := []string{"http://127.0.0.1:1/dead", server.URL, server.URL}

	results := c.GetRestHandler().BatchGet(context.Background(), urls, nil)
	defer closeBodies(results)

	if results[0].Err == nil {
		t.Fatal("dead host should have failed")
	}
	for i := 1; i < len(results); i++ {
		if !errors.Is(results[i].Err, requester.ErrBatchAborted) {
			t.Fatalf("result %d: err = %v, want requester.ErrBatchAborted", i, results[i].Err)
		}
		if results[i].Request == nil {
			t.Fatalf("result %d: skipped entry has no Request", i)
		}
	}
}

// TestSharedBodyReachesEveryRequest guards the io.Reader that used to be
// consumed by whichever request read it first.
func TestSharedBodyReachesEveryRequest(t *testing.T) {
	var mtx sync.Mutex
	var bodies []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		payload, _ := io.ReadAll(r.Body)
		mtx.Lock()
		bodies = append(bodies, string(payload))
		mtx.Unlock()
	}))
	defer server.Close()

	c := newTestConn(t, false, func(s *requester.ConnSettings) { s.SetUseAsync(true) })
	urls := []string{server.URL, server.URL, server.URL}

	results := c.DoManualBatchRequests(context.Background(), requester.POST, urls, nil, strings.NewReader("payload"))
	defer closeBodies(results)

	for i, r := range results {
		if r.Err != nil {
			t.Fatalf("result %d: %v", i, r.Err)
		}
	}
	mtx.Lock()
	defer mtx.Unlock()
	if len(bodies) != 3 {
		t.Fatalf("server saw %d bodies, want 3", len(bodies))
	}
	for i, body := range bodies {
		if body != "payload" {
			t.Fatalf("body %d = %q, want %q", i, body, "payload")
		}
	}
}

// TestRetriesAreNotMultiplied guards the duplicated retry loop: retrying in
// both DoRequest and the round tripper turned MaxRetries=2 into 9 attempts.
func TestRetriesAreNotMultiplied(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()

	var mtx sync.Mutex
	attempts := 0
	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			mtx.Lock()
			attempts++
			mtx.Unlock()
			conn.Close()
		}
	}()

	c := newTestConn(t, true, func(s *requester.ConnSettings) {
		s.SetMaxRetries(2)
		s.SetRetryDelay(10 * time.Millisecond)
	})

	start := time.Now()
	resp, err := c.GetRestHandler().Get(context.Background(), "http://"+listener.Addr().String(), nil)
	elapsed := time.Since(start)
	if err == nil {
		resp.Close()
		t.Fatal("expected the request to fail")
	}

	mtx.Lock()
	got := attempts
	mtx.Unlock()
	if got != 3 {
		t.Fatalf("attempts = %d, want 3 (1 try + 2 retries)", got)
	}
	// Two gaps of RetryDelay must actually have been waited out.
	if elapsed < 20*time.Millisecond {
		t.Fatalf("elapsed = %v, retries did not honour RetryDelay", elapsed)
	}
	if m := c.GetMetrics(); m.TotalRetries != 2 {
		t.Fatalf("TotalRetries = %d, want 2", m.TotalRetries)
	}
}

// TestHeadersAppliedOnce checks the Use* flags gate header injection and that
// it happens in exactly one layer.
func TestHeadersAppliedOnce(t *testing.T) {
	var auth, custom []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		auth = r.Header.Values("Authorization")
		custom = r.Header.Values("X-Custom")
	}))
	defer server.Close()

	c := newTestConn(t, false, func(s *requester.ConnSettings) {
		s.SetUseApiKey(true)
		s.SetApiKey(requester.ApiKey{Header: "Authorization", Value: "Bearer secret"})
		s.SetUseCachedHeaders(true)
		s.SetCacheHeaders(map[string]string{"X-Custom": "v"})
	})
	resp, err := c.GetRestHandler().Get(context.Background(), server.URL, nil)
	if err != nil {
		t.Fatal(err)
	}
	resp.Close()

	if len(auth) != 1 || auth[0] != "Bearer secret" {
		t.Fatalf("Authorization = %v", auth)
	}
	if len(custom) != 1 || custom[0] != "v" {
		t.Fatalf("X-Custom = %v", custom)
	}

	// With the flags off, nothing is injected.
	off := newTestConn(t, false, func(s *requester.ConnSettings) {
		s.SetApiKey(requester.ApiKey{Header: "Authorization", Value: "Bearer secret"})
		s.SetCacheHeaders(map[string]string{"X-Custom": "v"})
	})
	resp, err = off.GetRestHandler().Get(context.Background(), server.URL, nil)
	if err != nil {
		t.Fatal(err)
	}
	resp.Close()
	if len(auth) != 0 || len(custom) != 0 {
		t.Fatalf("headers leaked with flags off: %v %v", auth, custom)
	}
}

// TestStringWithoutMetrics guards the nil-pointer dereference in String().
func TestStringWithoutMetrics(t *testing.T) {
	c := newTestConn(t, false, nil)
	if got := c.String(); !strings.Contains(got, "metrics: disabled") {
		t.Fatalf("String() = %q", got)
	}
	if c.GetMetrics() != nil {
		t.Fatal("GetMetrics should be nil when metrics are disabled")
	}

	withMetrics := newTestConn(t, true, nil)
	if got := withMetrics.String(); !strings.Contains(got, "requests: 0") {
		t.Fatalf("String() = %q", got)
	}
}

// TestMetricsRecorded checks useMetrics actually records something.
func TestMetricsRecorded(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	defer server.Close()

	c := newTestConn(t, true, nil)
	resp, err := c.GetRestHandler().Get(context.Background(), server.URL, nil)
	if err != nil {
		t.Fatal(err)
	}
	resp.Discard()
	resp.Close()

	if _, err := c.GetRestHandler().Get(context.Background(), "http://127.0.0.1:1/dead", nil); err == nil {
		t.Fatal("expected a failure")
	}

	m := c.GetMetrics()
	if m.TotalRequests != 2 || m.SuccessfulRequests != 1 || m.FailedRequests != 1 {
		t.Fatalf("metrics = %+v", m)
	}
	if m.TotalConnectionErrors != 1 {
		t.Fatalf("TotalConnectionErrors = %d, want 1", m.TotalConnectionErrors)
	}
	if m.LastUsedAt.Before(m.CreatedAt) {
		t.Fatal("LastUsedAt was never updated")
	}
}

// TestMetricsCloneIsSafe guards the self-deadlock in the old Copy method.
func TestMetricsCloneIsSafe(t *testing.T) {
	m := requester.NewConnMetrics()
	m.TotalRequests = 7

	done := make(chan struct{})
	go func() {
		defer close(done)
		clone := m.Clone()
		if clone.TotalRequests != 7 {
			t.Errorf("clone.TotalRequests = %d", clone.TotalRequests)
		}
		// Cloning a clone must not deadlock either.
		_ = clone.Clone()
	}()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Clone deadlocked")
	}
}

// TestWarmUpOpensDistinctConns checks the pool really gets connNumber sockets.
func TestWarmUpOpensDistinctConns(t *testing.T) {
	server, conns := serveCountingConns(t, func(w http.ResponseWriter, r *http.Request) {})

	c := newTestConn(t, false, nil)
	if err := c.ConnectionWarmUp(context.Background(), server.URL, 4); err != nil {
		t.Fatal(err)
	}
	if got := conns.Load(); got != 4 {
		t.Fatalf("connections opened = %d, want 4", got)
	}
	if c.Status() != requester.Connected {
		t.Fatal("status not updated")
	}

	c.Close()
	if c.Status() != requester.Disconnected {
		t.Fatal("status not reset")
	}
	resp, err := c.GetRestHandler().Get(context.Background(), server.URL, nil)
	if err != nil {
		t.Fatal(err)
	}
	resp.Close()
	if got := conns.Load(); got != 5 {
		t.Fatalf("after Close, connections = %d, want 5 (pool was not dropped)", got)
	}
}

func TestWarmUpRejectsBadInput(t *testing.T) {
	c := newTestConn(t, false, nil)
	if err := c.ConnectionWarmUp(context.Background(), "http://x", 0); err == nil {
		t.Fatal("expected an error for connNumber = 0")
	}
	noKeepAlive := newTestConn(t, false, func(s *requester.ConnSettings) { s.SetUseKeepAlive(false) })
	if err := noKeepAlive.ConnectionWarmUp(context.Background(), "http://x", 2); err == nil {
		t.Fatal("expected an error when keep-alive is off")
	}
}

func TestSettingsCloneIsDeep(t *testing.T) {
	original := requester.NewConnSettings()
	original.SetCacheHeaders(map[string]string{"A": "1"})
	original.SetProtocols([]string{"h2"})

	clone := original.Clone()
	clone.SetCacheHeaders(map[string]string{"A": "2"})
	clone.SetProtocols([]string{"http/1.1"})

	if original.CacheHeaders()["A"] != "1" {
		t.Fatal("CacheHeaders is shared with the clone")
	}
	if original.Protocols()[0] != "h2" {
		t.Fatal("Protocols is shared with the clone")
	}
}

// TestSettingsAccessorsCopy checks the map and slice accessors copy in both
// directions, so no reference to the guarded state escapes the lock.
func TestSettingsAccessorsCopy(t *testing.T) {
	settings := requester.NewConnSettings()

	// A map handed to the setter must not stay live.
	source := map[string]string{"A": "1"}
	settings.SetCacheHeaders(source)
	source["A"] = "mutated"
	if settings.CacheHeaders()["A"] != "1" {
		t.Fatal("SetCacheHeaders kept the caller's map")
	}

	// A map handed back by the getter must not be live either.
	settings.CacheHeaders()["A"] = "mutated"
	if settings.CacheHeaders()["A"] != "1" {
		t.Fatal("CacheHeaders returned the live map")
	}

	protocols := []string{"h2"}
	settings.SetProtocols(protocols)
	protocols[0] = "mutated"
	if settings.Protocols()[0] != "h2" {
		t.Fatal("SetProtocols kept the caller's slice")
	}
	settings.Protocols()[0] = "mutated"
	if settings.Protocols()[0] != "h2" {
		t.Fatal("Protocols returned the live slice")
	}
}

// TestSettingsConcurrentAccess is meaningful under -race: the accessors must
// serialise readers and writers on the same settings value.
func TestSettingsConcurrentAccess(t *testing.T) {
	settings := requester.NewConnSettings()

	var wg sync.WaitGroup
	for i := range 8 {
		wg.Go(func() {
			for j := range 200 {
				settings.SetMaxRetries(uint64(j))
				settings.SetCacheHeaders(map[string]string{"i": strconv.Itoa(i)})
				_ = settings.MaxRetries()
				_ = settings.CacheHeaders()
				_ = settings.Clone()
				if _, err := json.Marshal(settings); err != nil {
					t.Errorf("marshal: %v", err)
					return
				}
			}
		})
	}
	wg.Wait()
}

func TestSettingsJSONRoundTrip(t *testing.T) {
	original := requester.NewConnSettings()
	original.SetMaxIdleConns(11)
	original.SetMaxIdleConnsPerHost(12)
	original.SetMaxConnsPerHost(13)
	original.SetMaxRedirects(14)
	original.SetMaxRetries(15)
	original.SetRetryDelay(750 * time.Millisecond)
	original.SetTimeout(90 * time.Second)
	original.SetHandShakeTimeout(7 * time.Second)
	original.SetMaxResponseBodySize(4096)
	original.SetUseProxy(true)
	original.SetUseCookies(true)
	original.SetUseKeepAlive(true)
	original.SetUseFailFast(true)
	original.SetUseHTTP2(true)
	original.SetUseAsync(true)
	original.SetUseApiKey(true)
	original.SetUseCachedHeaders(true)
	original.SetUseLogging(true)
	original.SetUseMetrics(true)
	original.SetAllowRedirects(true)
	original.SetCacheCookies(true)
	original.SetSkipPeerVerification(true)
	original.SetLogLevel(requester.DEBUG)
	original.SetProxyURL("http://proxy.example:8080")
	original.SetHttpVersion("2")
	original.SetApiKey(requester.ApiKey{Header: "X-API-Key", Value: "secret"})
	original.SetCacheHeaders(map[string]string{"X-Custom": "v"})
	original.SetProtocols([]string{"h2", "http/1.1"})

	encoded, err := json.Marshal(original)
	if err != nil {
		t.Fatal(err)
	}

	// Durations are readable strings rather than nanosecond counts.
	if !strings.Contains(string(encoded), `"retryDelay":"750ms"`) {
		t.Fatalf("retryDelay not encoded as a duration string: %s", encoded)
	}
	// A cookie jar is live state, not configuration.
	if strings.Contains(string(encoded), "cookieJar") {
		t.Fatalf("cookieJar leaked into the JSON: %s", encoded)
	}

	decoded := requester.NewConnSettings()
	if err := json.Unmarshal(encoded, decoded); err != nil {
		t.Fatal(err)
	}

	// Re-encoding must produce identical bytes; a field missing from the JSON
	// mirror would drop out here.
	reencoded, err := json.Marshal(decoded)
	if err != nil {
		t.Fatal(err)
	}
	if string(encoded) != string(reencoded) {
		t.Fatalf("round trip changed the settings:\n%s\n%s", encoded, reencoded)
	}

	// Spot-check that values really survived rather than all being zero.
	if decoded.MaxIdleConns() != 11 || decoded.RetryDelay() != 750*time.Millisecond {
		t.Fatalf("decoded = %d, %v", decoded.MaxIdleConns(), decoded.RetryDelay())
	}
	if decoded.LogLevel() != requester.DEBUG {
		t.Fatalf("decoded LogLevel = %v", decoded.LogLevel())
	}
	if got := decoded.ApiKey(); got.Header != "X-API-Key" || got.Value != "secret" {
		t.Fatalf("decoded ApiKey = %+v", got)
	}
	if decoded.CacheHeaders()["X-Custom"] != "v" || len(decoded.Protocols()) != 2 {
		t.Fatalf("decoded headers/protocols = %v, %v", decoded.CacheHeaders(), decoded.Protocols())
	}

	// Keys the document omits keep the value they already had.
	partial := requester.NewConnSettings()
	if err := json.Unmarshal([]byte(`{"maxRetries":9}`), partial); err != nil {
		t.Fatal(err)
	}
	if partial.MaxRetries() != 9 {
		t.Fatalf("MaxRetries = %d", partial.MaxRetries())
	}
	if partial.Timeout() != 30*time.Second {
		t.Fatalf("an absent key clobbered the default: Timeout = %v", partial.Timeout())
	}

	// Durations also accept a raw nanosecond count.
	if err := json.Unmarshal([]byte(`{"timeout":1500000000}`), partial); err != nil {
		t.Fatal(err)
	}
	if partial.Timeout() != 1500*time.Millisecond {
		t.Fatalf("numeric duration = %v", partial.Timeout())
	}
}

func TestGetCopySettingsDoesNotExposeLiveState(t *testing.T) {
	c := newTestConn(t, false, nil)

	got := c.GetCopySettings()
	got.SetMaxRetries(99)
	got.SetCacheHeaders(map[string]string{"X-Injected": "1"})

	// A second call must be unaffected by edits to the first.
	again := c.GetCopySettings()
	if again.MaxRetries() == 99 {
		t.Fatal("GetCopySettings handed out the live settings")
	}
	if _, injected := again.CacheHeaders()["X-Injected"]; injected {
		t.Fatal("GetCopySettings shared the CacheHeaders map")
	}

	// WithSettings copies too, so mutating what you passed in afterwards
	// cannot reconfigure a connection that is already built.
	settings := requester.NewConnSettings()
	built, err := requester.NewConnection(2, requester.WithSettings(settings))
	if err != nil {
		t.Fatal(err)
	}
	settings.SetMaxRetries(77)
	if built.GetCopySettings().MaxRetries() == 77 {
		t.Fatal("the connection kept a live reference to the caller's settings")
	}
}

func TestSettingsFromMap(t *testing.T) {
	settings, err := requester.NewConnSettingsFromMap(map[string]any{
		"MaxIdleConns": 42,
		"Timeout":      "45s",
		"RetryDelay":   250 * time.Millisecond,
		"UseAsync":     true,
		"LogLevel":     "debug",
		"Protocols":    []string{"h2"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if settings.MaxIdleConns() != 42 {
		t.Fatalf("MaxIdleConns = %d", settings.MaxIdleConns())
	}
	if settings.Timeout() != 45*time.Second {
		t.Fatalf("Timeout = %v", settings.Timeout())
	}
	if settings.RetryDelay() != 250*time.Millisecond {
		t.Fatalf("RetryDelay = %v", settings.RetryDelay())
	}
	if !settings.UseAsync() || settings.LogLevel() != requester.DEBUG {
		t.Fatalf("settings = %+v", settings)
	}
	// Untouched keys keep their defaults.
	if settings.MaxConnsPerHost() != 30 {
		t.Fatalf("MaxConnsPerHost = %d", settings.MaxConnsPerHost())
	}

	// A typo used to be silently ignored.
	if _, err := requester.NewConnSettingsFromMap(map[string]any{"MaxIdleConn": 5}); err == nil {
		t.Fatal("expected an error for an unknown key")
	}
	// So did a value of the wrong type.
	if _, err := requester.NewConnSettingsFromMap(map[string]any{"MaxIdleConns": "many"}); err == nil {
		t.Fatal("expected an error for a bad value type")
	}
	if _, err := requester.NewConnSettingsFromMap(map[string]any{"MaxIdleConns": -1}); err == nil {
		t.Fatal("expected an error for a negative count")
	}
}

// deadlineSeen registers a middleware that reports how long each request's
// context had left when it passed through.
func deadlineSeen(t *testing.T, c *requester.Connection) <-chan time.Duration {
	t.Helper()
	seen := make(chan time.Duration, 1)
	_, err := c.GetMiddleware().Add(func(req *http.Request, next requester.RoundTripFunc) (*http.Response, error) {
		deadline, ok := req.Context().Deadline()
		if !ok {
			seen <- -1
		} else {
			seen <- time.Until(deadline)
		}
		return next(req)
	})
	if err != nil {
		t.Fatal(err)
	}
	return seen
}

func TestZeroTimeoutFallsBackToDefault(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	defer server.Close()

	c := newTestConn(t, false, func(s *requester.ConnSettings) {
		s.SetTimeout(0)
		s.SetHandShakeTimeout(0)
	})
	seen := deadlineSeen(t, c)
	resp, err := c.GetRestHandler().Get(context.Background(), server.URL, nil)
	if err != nil {
		t.Fatal(err)
	}
	resp.Close()

	// The timeout is carried by the request context, not http.Client.Timeout.
	if left := <-seen; left < 29*time.Second || left > 30*time.Second {
		t.Fatalf("request deadline %v away, want the 30s default", left)
	}
	if c.GetClient().Timeout != 0 {
		t.Fatalf("client.Timeout = %v, want 0: the context carries it", c.GetClient().Timeout)
	}
	if c.GetTransport().TLSHandshakeTimeout != 10*time.Second {
		t.Fatalf("TLSHandshakeTimeout = %v", c.GetTransport().TLSHandshakeTimeout)
	}
}

// TestTimeoutCoversHeadersAndBody checks the context-based timeout fires both
// while waiting for the response and while reading a body that stalls, as
// http.Client.Timeout used to.
func TestTimeoutCoversHeadersAndBody(t *testing.T) {
	release := make(chan struct{})
	defer close(release)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("stall") == "body" {
			w.Header().Set("Content-Length", "10")
			w.Write([]byte("12345"))
			w.(http.Flusher).Flush()
		}
		select {
		case <-release:
		case <-r.Context().Done():
		}
	}))
	defer server.Close()

	c := newTestConn(t, false, func(s *requester.ConnSettings) { s.SetTimeout(150 * time.Millisecond) })

	start := time.Now()
	_, err := c.GetRestHandler().Get(context.Background(), server.URL+"?stall=headers", nil)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("stalled headers: err = %v, want DeadlineExceeded", err)
	}
	if took := time.Since(start); took > 2*time.Second {
		t.Fatalf("stalled headers took %v to time out", took)
	}

	resp, err := c.GetRestHandler().Get(context.Background(), server.URL+"?stall=body", nil)
	if err != nil {
		t.Fatalf("stalled body: headers should arrive, got %v", err)
	}
	defer resp.Close()
	if _, err := resp.Bytes(); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("stalled body: err = %v, want DeadlineExceeded", err)
	}
	if n := c.InFlight(); n != 0 {
		t.Fatalf("InFlight = %d after the timed-out body, want 0", n)
	}
}

// TestLayersDoNotLeakIntoCallerRequest checks that dropping the extra clones
// in the middleware stack and the header decorator did not let their edits
// reach the caller's own request.
func TestLayersDoNotLeakIntoCallerRequest(t *testing.T) {
	seen := make(chan http.Header, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		seen <- r.Header.Clone()
	}))
	defer server.Close()

	conn, err := requester.NewConnection(1,
		requester.WithApiKey("X-API-Key", "secret"),
		requester.WithHeaders(map[string]string{"Accept": "application/json"}),
		requester.WithMiddleware(func(req *http.Request, next requester.RoundTripFunc) (*http.Response, error) {
			req.Header.Set("X-From-Middleware", "1")
			return next(req)
		}),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Shutdown()

	original, err := http.NewRequestWithContext(context.Background(), http.MethodGet, server.URL, nil)
	if err != nil {
		t.Fatal(err)
	}
	original.Header.Set("X-Caller", "1")
	resp, err := conn.DoRequest(context.Background(), original)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()

	got := <-seen
	for _, key := range []string{"X-Caller", "X-From-Middleware", "X-Api-Key", "Accept"} {
		if got.Get(key) == "" {
			t.Errorf("server did not receive %s; got %v", key, got)
		}
	}
	if len(original.Header) != 1 || original.Header.Get("X-Caller") != "1" {
		t.Fatalf("the caller's request was modified: %v", original.Header)
	}
}

func TestRedirectPolicy(t *testing.T) {
	var hits int
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits++
		http.Redirect(w, r, "/next", http.StatusFound)
	}))
	defer server.Close()

	// Redirects disabled: the 302 is returned as-is.
	c := newTestConn(t, false, nil)
	resp, err := c.GetRestHandler().Get(context.Background(), server.URL, nil)
	if err != nil {
		t.Fatal(err)
	}
	resp.Close()
	if resp.StatusCode() != http.StatusFound {
		t.Fatalf("status = %d, want 302", resp.StatusCode())
	}

	// Redirects allowed: capped at MaxRedirects and counted.
	hits = 0
	follow := newTestConn(t, true, func(s *requester.ConnSettings) {
		s.SetAllowRedirects(true)
		s.SetMaxRedirects(3)
	})
	if _, err := follow.GetRestHandler().Get(context.Background(), server.URL, nil); err == nil {
		t.Fatal("expected the redirect cap to error")
	}
	if hits != 3 {
		t.Fatalf("server hits = %d, want 3", hits)
	}
	if m := follow.GetMetrics(); m.TotalRedirects != 2 {
		t.Fatalf("TotalRedirects = %d, want 2", m.TotalRedirects)
	}
}
