package tests

import (
	"context"
	"errors"
	"fmt"
	requester "jr_requester/jr_requester"
	"log/slog"
	"net/http"
	"net/http/cookiejar"
	"os"
	"strings"
	"testing"
	"time"
)

func TestOptionsApplyToSettings(t *testing.T) {
	jar, err := cookiejar.New(nil)
	if err != nil {
		t.Fatal(err)
	}

	conn, err := requester.NewConnection(1,
		requester.WithKeepAlive(16, 32),
		requester.WithMaxIdleConns(64),
		requester.WithTimeout(45*time.Second),
		requester.WithHandshakeTimeout(3*time.Second),
		requester.WithRetries(3, 750*time.Millisecond),
		requester.WithAsync(),
		requester.WithMetrics(),
		requester.WithApiKey("X-API-Key", "secret"),
		requester.WithHeaders(map[string]string{"X-Custom": "v"}),
		requester.WithRedirects(7),
		requester.WithMaxResponseBodySize(4096),
		requester.WithCookieJar(jar),
		requester.WithProxy("http://proxy.example:8080"),
		requester.WithHTTP2(),
		requester.WithProtocols("h2", "http/1.1"),
		requester.WithInsecureTLS(),
		requester.WithLogging(requester.DEBUG),
		requester.WithCompressionMode(requester.CompressionManual),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	s := conn.GetCopySettings()

	// Each option must set its paired Use* flag as well as the value: that
	// pairing is the whole point of the option layer.
	checks := []struct {
		name string
		got  any
		want any
	}{
		{"UseKeepAlive", s.UseKeepAlive(), true},
		{"MaxIdleConnsPerHost", s.MaxIdleConnsPerHost(), uint64(16)},
		{"MaxConnsPerHost", s.MaxConnsPerHost(), uint64(32)},
		{"MaxIdleConns", s.MaxIdleConns(), uint64(64)},
		{"Timeout", s.Timeout(), 45 * time.Second},
		{"HandShakeTimeout", s.HandShakeTimeout(), 3 * time.Second},
		{"MaxRetries", s.MaxRetries(), uint64(3)},
		{"RetryDelay", s.RetryDelay(), 750 * time.Millisecond},
		{"UseAsync", s.UseAsync(), true},
		{"UseMetrics", s.UseMetrics(), true},
		{"UseApiKey", s.UseApiKey(), true},
		{"ApiKey", s.ApiKey(), requester.ApiKey{Header: "X-API-Key", Value: "secret"}},
		{"UseCachedHeaders", s.UseCachedHeaders(), true},
		{"AllowRedirects", s.AllowRedirects(), true},
		{"MaxRedirects", s.MaxRedirects(), uint64(7)},
		{"MaxResponseBodySize", s.MaxResponseBodySize(), int64(4096)},
		{"UseCookies", s.UseCookies(), true},
		{"UseProxy", s.UseProxy(), true},
		{"ProxyURL", s.ProxyURL(), "http://proxy.example:8080"},
		{"UseHTTP2", s.UseHTTP2(), true},
		{"SkipPeerVerification", s.SkipPeerVerification(), true},
		{"UseLogging", s.UseLogging(), true},
		{"LogLevel", s.LogLevel(), requester.DEBUG},
	}
	for _, c := range checks {
		if c.got != c.want {
			t.Errorf("%s = %v, want %v", c.name, c.got, c.want)
		}
	}

	if s.CacheHeaders()["X-Custom"] != "v" {
		t.Errorf("CacheHeaders = %v", s.CacheHeaders())
	}
	if len(s.Protocols()) != 2 {
		t.Errorf("Protocols = %v", s.Protocols())
	}
	// The compression mode lives on the handler, not in the settings.
	if mode := conn.GetCompressionHandler().Mode(); mode != requester.CompressionManual {
		t.Errorf("compression mode = %v, want manual", mode)
	}
}

func TestNoOptionsGivesDefaults(t *testing.T) {
	conn, err := requester.NewConnection(1)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	s := conn.GetCopySettings()
	if s.MaxConnsPerHost() != 30 || s.Timeout() != 30*time.Second {
		t.Fatalf("defaults not applied: %d, %v", s.MaxConnsPerHost(), s.Timeout())
	}
	if s.SkipPeerVerification() {
		t.Fatal("TLS verification must be on by default")
	}
	// A nil option is a no-op rather than a panic.
	if _, err := requester.NewConnection(2, nil, requester.WithAsync(), nil); err != nil {
		t.Fatal(err)
	}
}

func TestWithSettingsMustComeFirst(t *testing.T) {
	base := requester.NewConnSettings()
	base.SetMaxRetries(5)

	// First is fine, and later options layer on top of it.
	conn, err := requester.NewConnection(1,
		requester.WithSettings(base),
		requester.WithAsync(),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	if s := conn.GetCopySettings(); s.MaxRetries() != 5 || !s.UseAsync() {
		t.Fatalf("settings = %d retries, async %v", s.MaxRetries(), s.UseAsync())
	}

	// Anywhere else it would discard the options before it, so it is rejected.
	_, err = requester.NewConnection(2,
		requester.WithAsync(),
		requester.WithSettings(base),
	)
	if !errors.Is(err, requester.ErrSettingsNotFirst) {
		t.Fatalf("err = %v, want ErrSettingsNotFirst", err)
	}

	// Including when it is the second WithSettings.
	_, err = requester.NewConnection(3,
		requester.WithSettings(base),
		requester.WithSettings(base),
	)
	if !errors.Is(err, requester.ErrSettingsNotFirst) {
		t.Fatalf("err = %v, want ErrSettingsNotFirst", err)
	}

	// A nil option ahead of it does not count as an applied option.
	if _, err := requester.NewConnection(4, nil, requester.WithSettings(base)); err != nil {
		t.Fatalf("a leading nil option blocked WithSettings: %v", err)
	}
}

func TestWithSettingsCopies(t *testing.T) {
	base := requester.NewConnSettings()
	conn, err := requester.NewConnection(1, requester.WithSettings(base))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	base.SetMaxRetries(99)
	if conn.GetCopySettings().MaxRetries() == 99 {
		t.Fatal("WithSettings kept a live reference to the caller's settings")
	}
}

func TestOptionValidation(t *testing.T) {
	cases := []struct {
		name   string
		option requester.ConnOption
	}{
		{"nil settings", requester.WithSettings(nil)},
		{"nil tune func", requester.WithSettingsFunc(nil)},
		{"nil logger", requester.WithLogger(nil)},
		{"unknown log level", requester.WithLogging(requester.LogLevel("LOUD"))},
		{"zero idle per host", requester.WithKeepAlive(0, 10)},
		{"max below idle", requester.WithKeepAlive(10, 5)},
		{"zero total idle", requester.WithMaxIdleConns(0)},
		{"zero timeout", requester.WithTimeout(0)},
		{"negative timeout", requester.WithTimeout(-time.Second)},
		{"zero handshake timeout", requester.WithHandshakeTimeout(0)},
		{"zero retries", requester.WithRetries(0, time.Second)},
		{"negative retry delay", requester.WithRetries(2, -time.Second)},
		{"empty api key header", requester.WithApiKey("   ", "secret")},
		{"empty api key value", requester.WithApiKey("X-API-Key", "")},
		{"invalid api key header", requester.WithApiKey("X API Key", "secret")},
		{"empty headers", requester.WithHeaders(nil)},
		{"blank header name", requester.WithHeaders(map[string]string{"": "v"})},
		{"invalid header name", requester.WithHeaders(map[string]string{"X Custom": "v"})},
		{"zero redirects", requester.WithRedirects(0)},
		{"zero body size", requester.WithMaxResponseBodySize(0)},
		{"nil cookie jar", requester.WithCookieJar(nil)},
		{"empty proxy", requester.WithProxy("  ")},
		{"unparsable proxy", requester.WithProxy("http://[::1")},
		{"schemeless proxy", requester.WithProxy("proxy.example:8080")},
		{"no protocols", requester.WithProtocols()},
		{"unknown protocol", requester.WithProtocols("h2", "quic")},
		{"unknown compression mode", requester.WithCompressionMode(requester.CompressionMode(42))},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			conn, err := requester.NewConnection(1, c.option)
			if err == nil {
				conn.Close()
				t.Fatal("expected the option to be rejected")
			}
			if !errors.Is(err, requester.ErrInvalidOption) && !errors.Is(err, requester.ErrSettingsNotFirst) {
				t.Fatalf("err = %v, want ErrInvalidOption", err)
			}
			if conn != nil {
				t.Fatal("a failed construction must not return a connection")
			}
		})
	}
}

// TestOptionErrorsAreCollected checks every bad option is reported, not just
// the first, so one round of fixes clears them all.
func TestOptionErrorsAreCollected(t *testing.T) {
	_, err := requester.NewConnection(1,
		requester.WithTimeout(0),
		requester.WithApiKey("", ""),
		requester.WithProtocols("quic"),
	)
	if err == nil {
		t.Fatal("expected errors")
	}

	message := err.Error()
	for _, want := range []string{"WithTimeout", "WithApiKey", "WithProtocols"} {
		if !strings.Contains(message, want) {
			t.Errorf("error does not mention %s: %v", want, err)
		}
	}
}

// TestOptionsAreOrdered checks a later option overrides an earlier one.
func TestOptionsAreOrdered(t *testing.T) {
	conn, err := requester.NewConnection(1,
		requester.WithTimeout(10*time.Second),
		requester.WithTimeout(20*time.Second),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	if got := conn.GetCopySettings().Timeout(); got != 20*time.Second {
		t.Fatalf("Timeout = %v, want the later option to win", got)
	}
}

func TestWithLoggerBypassesLogLevel(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))
	conn, err := requester.NewConnection(1, requester.WithLogger(logger))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	// UseLogging stays off; the supplied logger is used regardless.
	if conn.GetCopySettings().UseLogging() {
		t.Fatal("WithLogger should not flip UseLogging")
	}
}

func TestWithProxyFromEnvironment(t *testing.T) {
	conn, err := requester.NewConnection(1, requester.WithProxyFromEnvironment())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	s := conn.GetCopySettings()
	if !s.UseProxy() || s.ProxyURL() != "" {
		t.Fatalf("UseProxy = %v, ProxyURL = %q", s.UseProxy(), s.ProxyURL())
	}
}

// TestWithKeepAliveRaisesGlobalIdle checks the global ceiling is lifted to at
// least the per-host count, since a lower one would cap it silently.
func TestWithKeepAliveRaisesGlobalIdle(t *testing.T) {
	conn, err := requester.NewConnection(1, requester.WithKeepAlive(40, 80))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	if got := conn.GetCopySettings().MaxIdleConns(); got < 40 {
		t.Fatalf("MaxIdleConns = %d, want at least the per-host 40", got)
	}

	// An explicit WithMaxIdleConns afterwards still wins.
	conn2, err := requester.NewConnection(2,
		requester.WithKeepAlive(40, 80),
		requester.WithMaxIdleConns(100),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer conn2.Close()
	if got := conn2.GetCopySettings().MaxIdleConns(); got != 100 {
		t.Fatalf("MaxIdleConns = %d, want 100", got)
	}
}

// TestWithKeepAliveUnlimitedTotal checks maxPerHost = 0 stays "no limit"
// rather than being rejected as below idlePerHost.
func TestWithKeepAliveUnlimitedTotal(t *testing.T) {
	conn, err := requester.NewConnection(1, requester.WithKeepAlive(8, 0))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	if got := conn.GetCopySettings().MaxConnsPerHost(); got != 0 {
		t.Fatalf("MaxConnsPerHost = %d, want 0 (unlimited)", got)
	}
}

// TestApiKeyUsesCallerHeader is the point of the key/value pair: the header
// name comes from the caller, not from the library.
func TestApiKeyUsesCallerHeader(t *testing.T) {
	var seen http.Header
	server := serve(t, func(w http.ResponseWriter, r *http.Request) {
		seen = r.Header.Clone()
	})

	// A custom header, with nothing sent as Authorization.
	conn, err := requester.NewConnection(1, requester.WithApiKey("X-API-Key", "secret"))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	resp, err := conn.GetRestHandler().Get(context.Background(), server.URL, nil)
	if err != nil {
		t.Fatal(err)
	}
	resp.Close()

	if got := seen.Get("X-API-Key"); got != "secret" {
		t.Fatalf("X-API-Key = %q, want secret", got)
	}
	if got := seen.Get("Authorization"); got != "" {
		t.Fatalf("Authorization = %q, the library should not pick the header", got)
	}

	// The bearer scheme is just one caller-supplied value among others.
	bearer, err := requester.NewConnection(2, requester.WithApiKey("Authorization", "Bearer token"))
	if err != nil {
		t.Fatal(err)
	}
	defer bearer.Close()

	resp, err = bearer.GetRestHandler().Get(context.Background(), server.URL, nil)
	if err != nil {
		t.Fatal(err)
	}
	resp.Close()

	if got := seen.Get("Authorization"); got != "Bearer token" {
		t.Fatalf("Authorization = %q", got)
	}
	if len(seen.Values("Authorization")) != 1 {
		t.Fatalf("Authorization set more than once: %v", seen.Values("Authorization"))
	}

	// A header on the request itself still wins.
	resp, err = bearer.GetRestHandler().Get(context.Background(), server.URL,
		map[string]string{"Authorization": "Bearer override"})
	if err != nil {
		t.Fatal(err)
	}
	resp.Close()

	if got := seen.Get("Authorization"); got != "Bearer override" {
		t.Fatalf("Authorization = %q, the per-request header should win", got)
	}
}

// TestApiKeyIsRedactedWhenPrinted checks the value cannot be logged by accident.
func TestApiKeyIsRedactedWhenPrinted(t *testing.T) {
	key := requester.ApiKey{Header: "X-API-Key", Value: "super-secret"}

	printed := fmt.Sprintf("%v", key)
	if strings.Contains(printed, "super-secret") {
		t.Fatalf("the value leaked into %q", printed)
	}
	if !strings.Contains(printed, "X-API-Key") {
		t.Fatalf("the header name should still be visible: %q", printed)
	}

	if !(requester.ApiKey{}).IsZero() {
		t.Fatal("the zero value should report IsZero")
	}
	// Half a pair is useless, so it counts as unset either way round.
	if !(requester.ApiKey{Header: "X-API-Key"}).IsZero() {
		t.Fatal("a header with no value should report IsZero")
	}
	if !(requester.ApiKey{Value: "secret"}).IsZero() {
		t.Fatal("a value with no header should report IsZero")
	}
}
