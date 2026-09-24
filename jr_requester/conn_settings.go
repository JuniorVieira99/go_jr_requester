package jr_requester

import (
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"net"
	"net/http"
	"net/http/cookiejar"
	"net/url"
	"slices"
	"strings"
	"sync"
	"time"
)

// ------------
// Structs
// ------------

// ConnSettings holds the configuration settings for a connection.
//
// The fields are unexported and reached through the accessors in
// conn_settings_accessors.go, each of which takes the embedded lock, so a
// settings value can be shared and returned across goroutines.
//
// ConnSettings marshals to and from JSON with the field names in the json
// tags on connSettingsJSON below. The tags have to live there rather than on
// these fields: encoding/json cannot see unexported fields, and go vet rejects
// a json tag on one outright. MarshalJSON and UnmarshalJSON bridge the two,
// and TestSettingsJSONRoundTrip fails if a field is ever added here without a
// matching entry in the mirror.
type ConnSettings struct {
	// mtx guards every field below it.
	mtx sync.RWMutex

	// Numerical Settings
	maxIdleConns        uint64
	maxIdleConnsPerHost uint64
	maxConnsPerHost     uint64
	maxRedirects        uint64
	maxRetries          uint64
	retryDelay          time.Duration
	timeout             time.Duration
	handShakeTimeout    time.Duration
	// maxResponseBodySize caps how many bytes Response.Bytes/Text/JSON will
	// hold in memory. Zero selects the default; a negative value means
	// unlimited. Response.Save and Response.Stream are never capped.
	maxResponseBodySize int64

	// Boolean Settings
	useProxy         bool
	useCookies       bool
	useKeepAlive     bool
	useFailFast      bool
	useHTTP2         bool
	useAsync         bool
	useApiKey        bool
	useCachedHeaders bool
	useLogging       bool

	// useMetrics turns on the ConnMetrics recording exposed by
	// Connection.GetMetrics. With it off, GetMetrics returns nil and no
	// counters are kept.
	useMetrics     bool
	allowRedirects bool
	cacheCookies   bool
	// skipPeerVerification disables TLS certificate verification. It is
	// phrased as an opt-out on purpose: a zero-value ConnSettings must give a
	// client that verifies certificates, so the unsafe choice has to be typed
	// out deliberately.
	skipPeerVerification bool

	// Cached Values
	logLevel     LogLevel
	proxyURL     string
	httpVersion  string
	apiKey       ApiKey
	cookieJar    http.CookieJar
	cacheHeaders map[string]string
	protocols    []string
}

// requestDecorator wraps a RoundTripper to apply the settings that cannot be
// expressed as http.Transport fields: cached headers, the API key and retries.
type requestDecorator struct {
	base       http.RoundTripper
	headers    map[string]string
	apiKey     ApiKey
	maxRetries uint64
	retryDelay time.Duration
	metrics    *ConnMetrics
}

// ------------
// Constructors
// ------------

// NewConnSettings makes a new ConnSettings with default values.
func NewConnSettings() *ConnSettings {

	return &ConnSettings{
		maxIdleConns:         15,
		maxIdleConnsPerHost:  15,
		maxConnsPerHost:      30,
		maxRedirects:         5,
		maxRetries:           0,
		retryDelay:           defaultRetryDelay,
		timeout:              defaultRequestTimeout,
		handShakeTimeout:     defaultHandshakeTimeout,
		maxResponseBodySize:  defaultMaxResponseBodySize,
		useProxy:             false,
		useCookies:           false,
		useKeepAlive:         false,
		useFailFast:          false,
		useHTTP2:             false,
		useAsync:             false,
		useApiKey:            false,
		useLogging:           false,
		useMetrics:           false,
		useCachedHeaders:     false,
		allowRedirects:       false,
		skipPeerVerification: false,
		cacheCookies:         false,
		logLevel:             INFO,
		proxyURL:             "",
		httpVersion:          "1.1",
		apiKey:               ApiKey{},
		cookieJar:            nil,
		cacheHeaders:         nil,
		protocols:            nil,
	}
}

// NewConnSettingsFromMap creates a new ConnSettings from a map, starting from
// the defaults and overriding the keys present. Unknown keys and values of the
// wrong type are reported rather than ignored, so a typo cannot silently leave
// a setting at its default.
//
// Keys are the exported accessor names ("MaxIdleConns", "UseAsync"). Durations
// accept a time.Duration or a string understood by time.ParseDuration ("30s",
// "1m500ms"). Integers accept any signed or unsigned integer type, plus
// float64 so JSON-decoded maps work.
func NewConnSettingsFromMap(settingsMap map[string]any) (*ConnSettings, error) {
	settings := NewConnSettings()
	var errs []error

	// assign runs set() and records a typed error naming the offending key.
	assign := func(key string, value any, set func(any) bool) {
		if !set(value) {
			errs = append(errs, fmt.Errorf("setting %q: cannot use %T as the expected type", key, value))
		}
	}

	for key, value := range settingsMap {
		switch key {
		case "MaxIdleConns":
			assign(key, value, func(v any) bool { return assignUint(v, &settings.maxIdleConns) })
		case "MaxIdleConnsPerHost":
			assign(key, value, func(v any) bool { return assignUint(v, &settings.maxIdleConnsPerHost) })
		case "MaxConnsPerHost":
			assign(key, value, func(v any) bool { return assignUint(v, &settings.maxConnsPerHost) })
		case "MaxRedirects":
			assign(key, value, func(v any) bool { return assignUint(v, &settings.maxRedirects) })
		case "MaxRetries":
			assign(key, value, func(v any) bool { return assignUint(v, &settings.maxRetries) })
		case "RetryDelay":
			assign(key, value, func(v any) bool { return assignDuration(v, &settings.retryDelay) })
		case "Timeout":
			assign(key, value, func(v any) bool { return assignDuration(v, &settings.timeout) })
		case "HandShakeTimeout":
			assign(key, value, func(v any) bool { return assignDuration(v, &settings.handShakeTimeout) })
		case "MaxResponseBodySize":
			assign(key, value, func(v any) bool { return assignInt(v, &settings.maxResponseBodySize) })

		case "UseProxy":
			assign(key, value, func(v any) bool { return assignBool(v, &settings.useProxy) })
		case "UseCookies":
			assign(key, value, func(v any) bool { return assignBool(v, &settings.useCookies) })
		case "UseKeepAlive":
			assign(key, value, func(v any) bool { return assignBool(v, &settings.useKeepAlive) })
		case "UseFailFast":
			assign(key, value, func(v any) bool { return assignBool(v, &settings.useFailFast) })
		case "UseHTTP2":
			assign(key, value, func(v any) bool { return assignBool(v, &settings.useHTTP2) })
		case "UseAsync":
			assign(key, value, func(v any) bool { return assignBool(v, &settings.useAsync) })
		case "UseApiKey":
			assign(key, value, func(v any) bool { return assignBool(v, &settings.useApiKey) })
		case "UseCachedHeaders":
			assign(key, value, func(v any) bool { return assignBool(v, &settings.useCachedHeaders) })
		case "UseLogging":
			assign(key, value, func(v any) bool { return assignBool(v, &settings.useLogging) })
		case "UseMetrics":
			assign(key, value, func(v any) bool { return assignBool(v, &settings.useMetrics) })
		case "AllowRedirects":
			assign(key, value, func(v any) bool { return assignBool(v, &settings.allowRedirects) })
		case "SkipPeerVerification":
			assign(key, value, func(v any) bool { return assignBool(v, &settings.skipPeerVerification) })
		case "CacheCookies":
			assign(key, value, func(v any) bool { return assignBool(v, &settings.cacheCookies) })

		case "LogLevel":
			switch v := value.(type) {
			case LogLevel:
				settings.logLevel = v
			case string:
				settings.logLevel = LogLevel(strings.ToUpper(strings.TrimSpace(v)))
			default:
				errs = append(errs, fmt.Errorf("setting %q: cannot use %T as LogLevel", key, value))
			}
		case "ProxyURL":
			assign(key, value, func(v any) bool { return assignString(v, &settings.proxyURL) })
		case "HttpVersion":
			assign(key, value, func(v any) bool { return assignString(v, &settings.httpVersion) })
		case "ApiKey":
			if v, ok := value.(ApiKey); ok {
				settings.apiKey = v
			} else {
				errs = append(errs, fmt.Errorf("setting %q: cannot use %T as ApiKey", key, value))
			}
		case "CookieJar":
			if v, ok := value.(http.CookieJar); ok {
				settings.cookieJar = v
			} else {
				errs = append(errs, fmt.Errorf("setting %q: cannot use %T as http.CookieJar", key, value))
			}
		case "CacheHeaders":
			if v, ok := value.(map[string]string); ok {
				settings.cacheHeaders = maps.Clone(v)
			} else {
				errs = append(errs, fmt.Errorf("setting %q: cannot use %T as map[string]string", key, value))
			}
		case "Protocols":
			if v, ok := value.([]string); ok {
				settings.protocols = slices.Clone(v)
			} else {
				errs = append(errs, fmt.Errorf("setting %q: cannot use %T as []string", key, value))
			}
		default:
			errs = append(errs, fmt.Errorf("unknown setting %q", key))
		}
	}

	if err := errors.Join(errs...); err != nil {
		return nil, err
	}
	return settings, nil
}

// Clone returns a deep copy of the settings. The header map and protocol slice
// are copied too, so mutating the clone cannot reach back into the original.
// CookieJar is shared by design: a jar is stateful and meant to be shared.
//
// The copy is made field by field rather than by dereferencing, because the
// embedded mutex must not be copied along with the values it guards.
func (cs *ConnSettings) Clone() *ConnSettings {
	if cs == nil {
		return nil
	}
	cs.mtx.RLock()
	defer cs.mtx.RUnlock()

	return &ConnSettings{
		maxIdleConns:         cs.maxIdleConns,
		maxIdleConnsPerHost:  cs.maxIdleConnsPerHost,
		maxConnsPerHost:      cs.maxConnsPerHost,
		maxRedirects:         cs.maxRedirects,
		maxRetries:           cs.maxRetries,
		retryDelay:           cs.retryDelay,
		timeout:              cs.timeout,
		handShakeTimeout:     cs.handShakeTimeout,
		maxResponseBodySize:  cs.maxResponseBodySize,
		useProxy:             cs.useProxy,
		useCookies:           cs.useCookies,
		useKeepAlive:         cs.useKeepAlive,
		useFailFast:          cs.useFailFast,
		useHTTP2:             cs.useHTTP2,
		useAsync:             cs.useAsync,
		useApiKey:            cs.useApiKey,
		useCachedHeaders:     cs.useCachedHeaders,
		useLogging:           cs.useLogging,
		useMetrics:           cs.useMetrics,
		allowRedirects:       cs.allowRedirects,
		cacheCookies:         cs.cacheCookies,
		skipPeerVerification: cs.skipPeerVerification,
		logLevel:             cs.logLevel,
		proxyURL:             cs.proxyURL,
		httpVersion:          cs.httpVersion,
		apiKey:               cs.apiKey,
		cookieJar:            cs.cookieJar,
		cacheHeaders:         maps.Clone(cs.cacheHeaders),
		protocols:            slices.Clone(cs.protocols),
	}
}

// ------------
// JSON
// ------------

// jsonDuration renders a duration as "30s" rather than a nanosecond count, and
// accepts either form when decoding.
type jsonDuration time.Duration

func (d jsonDuration) MarshalJSON() ([]byte, error) {
	return json.Marshal(time.Duration(d).String())
}

func (d *jsonDuration) UnmarshalJSON(data []byte) error {
	var text string
	if err := json.Unmarshal(data, &text); err == nil {
		parsed, err := time.ParseDuration(text)
		if err != nil {
			return fmt.Errorf("cannot parse %q as a duration: %w", text, err)
		}
		*d = jsonDuration(parsed)
		return nil
	}

	var nanoseconds int64
	if err := json.Unmarshal(data, &nanoseconds); err != nil {
		return fmt.Errorf("cannot decode %s as a duration", data)
	}
	*d = jsonDuration(nanoseconds)
	return nil
}

// connSettingsJSON mirrors ConnSettings with exported fields so encoding/json
// can see them. The tags here must match the ones on ConnSettings.
type connSettingsJSON struct {
	MaxIdleConns        uint64       `json:"maxIdleConns"`
	MaxIdleConnsPerHost uint64       `json:"maxIdleConnsPerHost"`
	MaxConnsPerHost     uint64       `json:"maxConnsPerHost"`
	MaxRedirects        uint64       `json:"maxRedirects"`
	MaxRetries          uint64       `json:"maxRetries"`
	RetryDelay          jsonDuration `json:"retryDelay"`
	Timeout             jsonDuration `json:"timeout"`
	HandShakeTimeout    jsonDuration `json:"handShakeTimeout"`
	MaxResponseBodySize int64        `json:"maxResponseBodySize"`

	UseProxy              bool `json:"useProxy"`
	UseCookies            bool `json:"useCookies"`
	UseKeepAlive          bool `json:"useKeepAlive"`
	UseFailFast           bool `json:"useFailFast"`
	UseHTTP2              bool `json:"useHTTP2"`
	UseCancellationTokens bool `json:"useCancellationTokens"`
	UseAsync              bool `json:"useAsync"`
	UseApiKey             bool `json:"useApiKey"`
	UseCachedHeaders      bool `json:"useCachedHeaders"`
	UseLogging            bool `json:"useLogging"`
	UseMetrics            bool `json:"useMetrics"`
	AllowRedirects        bool `json:"allowRedirects"`
	CacheCookies          bool `json:"cacheCookies"`
	SkipPeerVerification  bool `json:"skipPeerVerification"`

	LogLevel     LogLevel          `json:"logLevel"`
	ProxyURL     string            `json:"proxyURL"`
	HttpVersion  string            `json:"httpVersion"`
	ApiKey       ApiKey            `json:"apiKey"`
	CacheHeaders map[string]string `json:"cacheHeaders"`
	Protocols    []string          `json:"protocols"`
}

// snapshot copies the fields into the JSON mirror. The caller holds the lock.
func (cs *ConnSettings) snapshot() connSettingsJSON {
	return connSettingsJSON{
		MaxIdleConns:         cs.maxIdleConns,
		MaxIdleConnsPerHost:  cs.maxIdleConnsPerHost,
		MaxConnsPerHost:      cs.maxConnsPerHost,
		MaxRedirects:         cs.maxRedirects,
		MaxRetries:           cs.maxRetries,
		RetryDelay:           jsonDuration(cs.retryDelay),
		Timeout:              jsonDuration(cs.timeout),
		HandShakeTimeout:     jsonDuration(cs.handShakeTimeout),
		MaxResponseBodySize:  cs.maxResponseBodySize,
		UseProxy:             cs.useProxy,
		UseCookies:           cs.useCookies,
		UseKeepAlive:         cs.useKeepAlive,
		UseFailFast:          cs.useFailFast,
		UseHTTP2:             cs.useHTTP2,
		UseAsync:             cs.useAsync,
		UseApiKey:            cs.useApiKey,
		UseCachedHeaders:     cs.useCachedHeaders,
		UseLogging:           cs.useLogging,
		UseMetrics:           cs.useMetrics,
		AllowRedirects:       cs.allowRedirects,
		CacheCookies:         cs.cacheCookies,
		SkipPeerVerification: cs.skipPeerVerification,
		LogLevel:             cs.logLevel,
		ProxyURL:             cs.proxyURL,
		HttpVersion:          cs.httpVersion,
		ApiKey:               cs.apiKey,
		CacheHeaders:         maps.Clone(cs.cacheHeaders),
		Protocols:            slices.Clone(cs.protocols),
	}
}

// restore writes the mirror back into the fields. The caller holds the lock.
func (cs *ConnSettings) restore(mirror connSettingsJSON) {
	cs.maxIdleConns = mirror.MaxIdleConns
	cs.maxIdleConnsPerHost = mirror.MaxIdleConnsPerHost
	cs.maxConnsPerHost = mirror.MaxConnsPerHost
	cs.maxRedirects = mirror.MaxRedirects
	cs.maxRetries = mirror.MaxRetries
	cs.retryDelay = time.Duration(mirror.RetryDelay)
	cs.timeout = time.Duration(mirror.Timeout)
	cs.handShakeTimeout = time.Duration(mirror.HandShakeTimeout)
	cs.maxResponseBodySize = mirror.MaxResponseBodySize
	cs.useProxy = mirror.UseProxy
	cs.useCookies = mirror.UseCookies
	cs.useKeepAlive = mirror.UseKeepAlive
	cs.useFailFast = mirror.UseFailFast
	cs.useHTTP2 = mirror.UseHTTP2
	cs.useAsync = mirror.UseAsync
	cs.useApiKey = mirror.UseApiKey
	cs.useCachedHeaders = mirror.UseCachedHeaders
	cs.useLogging = mirror.UseLogging
	cs.useMetrics = mirror.UseMetrics
	cs.allowRedirects = mirror.AllowRedirects
	cs.cacheCookies = mirror.CacheCookies
	cs.skipPeerVerification = mirror.SkipPeerVerification
	cs.logLevel = mirror.LogLevel
	cs.proxyURL = mirror.ProxyURL
	cs.httpVersion = mirror.HttpVersion
	cs.apiKey = mirror.ApiKey
	cs.cacheHeaders = maps.Clone(mirror.CacheHeaders)
	cs.protocols = slices.Clone(mirror.Protocols)
}

// MarshalJSON encodes the settings. CookieJar is skipped: a jar is live state,
// not configuration. The ApiKey value is included, so treat the output as a
// secret.
func (cs *ConnSettings) MarshalJSON() ([]byte, error) {
	if cs == nil {
		return []byte("null"), nil
	}
	cs.mtx.RLock()
	defer cs.mtx.RUnlock()
	return json.Marshal(cs.snapshot())
}

// UnmarshalJSON decodes into the settings, leaving any key the document omits
// at its current value. Decode into NewConnSettings() to inherit the defaults.
func (cs *ConnSettings) UnmarshalJSON(data []byte) error {
	if cs == nil {
		return errors.New("cannot unmarshal into a nil ConnSettings")
	}
	cs.mtx.Lock()
	defer cs.mtx.Unlock()

	// Seed the mirror with what is already set so absent keys survive.
	mirror := cs.snapshot()
	if err := json.Unmarshal(data, &mirror); err != nil {
		return err
	}
	cs.restore(mirror)
	return nil
}

// ------------
// Helpers
// ------------
//
// These read the fields directly rather than through the locking accessors.
// They only ever run against the private clone NewConnection takes, which no
// other goroutine can reach, and nesting a read lock inside another one risks
// deadlocking against a waiting writer.

// effectiveTimeout is the overall per-request budget, falling back to a default
// when the setting is unset. A zero http.Client.Timeout means "wait forever",
// which is never what an unset setting should mean.
func (cs *ConnSettings) effectiveTimeout() time.Duration {
	if cs.timeout <= 0 {
		return defaultRequestTimeout
	}
	return cs.timeout
}

// effectiveHandshakeTimeout caps the TLS handshake, defaulting when unset.
func (cs *ConnSettings) effectiveHandshakeTimeout() time.Duration {
	if cs.handShakeTimeout <= 0 {
		return defaultHandshakeTimeout
	}
	return cs.handShakeTimeout
}

// effectiveMaxResponseBodySize is the buffering cap for response bodies. Zero
// means "unset" and takes the default; negative is an explicit "no limit".
func (cs *ConnSettings) effectiveMaxResponseBodySize() int64 {
	if cs.maxResponseBodySize == 0 {
		return defaultMaxResponseBodySize
	}
	if cs.maxResponseBodySize < 0 {
		return -1
	}
	return cs.maxResponseBodySize
}

// effectiveRetryDelay is the pause between attempts. A zero delay would spin
// through every retry back to back and hammer a struggling server.
func (cs *ConnSettings) effectiveRetryDelay() time.Duration {
	if cs.retryDelay <= 0 {
		return defaultRetryDelay
	}
	return cs.retryDelay
}

// proxyFunc resolves the proxy for every request. A configured but unparsable
// ProxyURL is reported and treated as "no proxy" so requests never silently
// bypass an intended proxy without a trace.
func (cs *ConnSettings) proxyFunc(logger *slog.Logger) func(*http.Request) (*url.URL, error) {
	if !cs.useProxy {
		return nil
	}
	if strings.TrimSpace(cs.proxyURL) == "" {
		return http.ProxyFromEnvironment
	}

	proxyURL, err := url.Parse(cs.proxyURL)
	if err != nil {
		logger.Error("invalid proxy URL, continuing without a proxy", "proxyURL", cs.proxyURL, "error", err)
		return nil
	}
	return http.ProxyURL(proxyURL)
}

// protocolSet maps Protocols/HttpVersion/UseHTTP2 onto an http.Protocols set.
func (cs *ConnSettings) protocolSet() *http.Protocols {
	protocols := new(http.Protocols)

	for _, name := range cs.protocols {
		switch strings.ToLower(strings.TrimSpace(name)) {
		case "1", "1.0", "1.1", "http/1.0", "http/1.1", "http1":
			protocols.SetHTTP1(true)
		case "2", "2.0", "h2", "http/2", "http2":
			protocols.SetHTTP2(true)
		case "h2c":
			protocols.SetUnencryptedHTTP2(true)
		}
	}

	if len(cs.protocols) == 0 {
		switch strings.ToLower(strings.TrimSpace(cs.httpVersion)) {
		case "2", "2.0", "h2", "http/2", "http2":
			protocols.SetHTTP2(true)
		case "h2c":
			protocols.SetUnencryptedHTTP2(true)
		default:
			protocols.SetHTTP1(true)
		}
	}

	if cs.useHTTP2 {
		protocols.SetHTTP2(true)
	}
	if !protocols.HTTP1() && !protocols.HTTP2() && !protocols.UnencryptedHTTP2() {
		protocols.SetHTTP1(true)
	}
	return protocols
}

// resolveCookieJar returns the jar to install on the client, if cookies are on.
func (cs *ConnSettings) resolveCookieJar(logger *slog.Logger) http.CookieJar {
	if !cs.useCookies {
		return nil
	}
	if cs.cookieJar != nil {
		return cs.cookieJar
	}
	if !cs.cacheCookies {
		return nil
	}

	jar, err := cookiejar.New(nil)
	if err != nil {
		logger.Error("could not create cookie jar, continuing without one", "error", err)
		return nil
	}
	return jar
}

// checkRedirect enforces AllowRedirects and MaxRedirects, and counts every
// redirect it lets through when metrics are enabled.
func (cs *ConnSettings) checkRedirect(metrics *ConnMetrics) func(*http.Request, []*http.Request) error {
	allow := cs.allowRedirects
	maxRedirects := cs.maxRedirects

	return func(req *http.Request, via []*http.Request) error {
		if !allow {
			return http.ErrUseLastResponse
		}
		if uint64(len(via)) >= maxRedirects {
			return fmt.Errorf("stopped after %d redirects", maxRedirects)
		}
		metrics.incrementRedirects()
		return nil
	}
}

// newTransport creates a new http.Transport based on the connection settings.
func (cs *ConnSettings) newTransport(logger *slog.Logger) *http.Transport {
	keepAlive := defaultKeepAliveInterval
	if !cs.useKeepAlive {
		// A negative interval disables TCP keep-alive probes entirely.
		keepAlive = -1
	}

	dialer := &net.Dialer{
		Timeout:   cs.effectiveTimeout(),
		KeepAlive: keepAlive,
	}

	// Fail fast: give up as soon as the server stalls on the response headers
	// instead of waiting out the whole request budget.
	responseHeaderTimeout := time.Duration(0)
	if cs.useFailFast {
		responseHeaderTimeout = cs.effectiveHandshakeTimeout()
	}

	return &http.Transport{
		Proxy:       cs.proxyFunc(logger),
		DialContext: dialer.DialContext,
		TLSClientConfig: &tls.Config{
			InsecureSkipVerify: cs.skipPeerVerification,
			MinVersion:         tls.VersionTLS12,
		},
		MaxIdleConns:          int(cs.maxIdleConns),
		MaxIdleConnsPerHost:   int(cs.maxIdleConnsPerHost),
		MaxConnsPerHost:       int(cs.maxConnsPerHost),
		IdleConnTimeout:       defaultIdleConnTimeout,
		TLSHandshakeTimeout:   cs.effectiveHandshakeTimeout(),
		ResponseHeaderTimeout: responseHeaderTimeout,
		ExpectContinueTimeout: defaultExpectContinueTimeout,
		DisableKeepAlives:     !cs.useKeepAlive,
		// Compression is owned by the CompressionHandler, not by a setting.
		// Leaving the transport's negotiation enabled is what makes the
		// handler's modes work: net/http only decompresses a body when it
		// added Accept-Encoding itself, so the header the handler injects is
		// enough to hand the body over still compressed.
		DisableCompression: false,
		ForceAttemptHTTP2:  cs.useHTTP2,
		Protocols:          cs.protocolSet(),
	}
}

// newRoundTripper wraps the transport with header injection and retries when
// the settings ask for them, and returns the transport untouched otherwise.
//
// This is the single place retries happen. Layering a second retry loop in the
// caller would multiply the attempts rather than add to them.
func (cs *ConnSettings) newRoundTripper(transport *http.Transport, metrics *ConnMetrics) http.RoundTripper {
	headers := map[string]string{}
	if cs.useCachedHeaders {
		maps.Copy(headers, cs.cacheHeaders)
	}

	var apiKey ApiKey
	if cs.useApiKey {
		apiKey = cs.apiKey
	}

	// Failing fast and retrying are contradictory; fail fast wins.
	maxRetries := cs.maxRetries
	if cs.useFailFast {
		maxRetries = 0
	}

	if len(headers) == 0 && apiKey.IsZero() && maxRetries == 0 {
		return transport
	}

	return &requestDecorator{
		base:       transport,
		headers:    headers,
		apiKey:     apiKey,
		maxRetries: maxRetries,
		retryDelay: cs.effectiveRetryDelay(),
		metrics:    metrics,
	}
}

// ------------
// Methods requestDecorator
// ------------

func (d *requestDecorator) RoundTrip(req *http.Request) (*http.Response, error) {
	// A RoundTripper must not mutate the request it is handed, unless it is
	// one of DoRequest's own private copies. Request.Context never returns
	// nil, so a clone always carries the caller's context.
	outgoing := req
	if !ownsRequest(req) {
		outgoing = req.Clone(req.Context())
	}
	if outgoing.Header == nil {
		outgoing.Header = make(http.Header)
	}

	for key, value := range d.headers {
		if outgoing.Header.Get(key) == "" {
			outgoing.Header.Set(key, value)
		}
	}

	// The caller chose the header, so this is not tied to Authorization.
	if !d.apiKey.IsZero() && outgoing.Header.Get(d.apiKey.Header) == "" {
		outgoing.Header.Set(d.apiKey.Header, d.apiKey.Value)
	}

	var lastErr error
	var timer *time.Timer
	defer func() {
		if timer != nil {
			timer.Stop()
		}
	}()

	for attempt := uint64(0); attempt <= d.maxRetries; attempt++ {
		if attempt > 0 {
			d.metrics.incrementRetries()
			if outgoing.Body != nil {
				// Only a rewindable body can be replayed.
				if outgoing.GetBody == nil {
					break
				}
				body, err := outgoing.GetBody()
				if err != nil {
					break
				}
				outgoing.Body = body
			}
		}

		response, err := d.base.RoundTrip(outgoing)
		if err == nil {
			return response, nil
		}
		lastErr = err

		// Never retry once the caller has cancelled or the deadline passed.
		if outgoing.Context().Err() != nil {
			break
		}

		// Delay before the next attempt, if any. The first attempt is immediate.
		if attempt < d.maxRetries {
			if timer == nil {
				timer = time.NewTimer(d.retryDelay)
			} else {
				timer.Reset(d.retryDelay)
			}
			select {
			// If the context is done, return immediately instead of waiting.
			case <-outgoing.Context().Done():
				return nil, outgoing.Context().Err()
			// If the timer fires, proceed to the next attempt.
			case <-timer.C:
			}
		}
	}
	return nil, lastErr
}
