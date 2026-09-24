package jr_requester

import (
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/url"
	"strings"
	"time"
)

// ------------
// Errors
// ------------

var (
	// ErrSettingsNotFirst is reported when WithSettings is not the first
	// option. It replaces the whole settings value, so anything applied
	// before it would be silently thrown away.
	ErrSettingsNotFirst = errors.New("WithSettings must be the first option")
	// ErrInvalidOption is the base for an option rejecting its argument.
	ErrInvalidOption = errors.New("invalid option")
)

// optionErrorf builds an ErrInvalidOption naming the offending option.
func optionErrorf(option string, format string, args ...any) error {
	return fmt.Errorf("%w: %s: %s", ErrInvalidOption, option, fmt.Sprintf(format, args...))
}

// ------------
// Structs
// ------------

// ConnOption configures a Connection at construction time. Options are applied
// in order and the later one wins, so a broad option can be narrowed by a
// specific one that follows it.
type ConnOption func(*connConfig) error

// connConfig is what the options write to. It is more than the settings: the
// logger and the compression mode are not ConnSettings fields.
type connConfig struct {
	settings        *ConnSettings
	logger          *slog.Logger
	compressionMode CompressionMode
	middleware      *MiddlewareStack

	// applied counts the options already run, so WithSettings can insist on
	// being the first of them.
	applied int
}

// ------------
// Base options
// ------------

// WithSettings seeds the connection from an existing ConnSettings, which is
// how a configuration loaded from JSON is used as the starting point:
//
//	conn, err := NewConnection(1, WithSettings(fromConfigFile), WithHTTP2())
//
// It replaces the whole settings value, so it must come first; a later
// WithSettings would discard every option before it, and is rejected rather
// than silently dropping them.
func WithSettings(settings *ConnSettings) ConnOption {
	return func(c *connConfig) error {
		if c.applied > 0 {
			return ErrSettingsNotFirst
		}
		if settings == nil {
			return optionErrorf("WithSettings", "settings must not be nil")
		}
		c.settings = settings.Clone()
		return nil
	}
}

// WithSettingsFunc is the escape hatch for the settings that have no dedicated
// option. It runs against the settings being built.
func WithSettingsFunc(tune func(*ConnSettings)) ConnOption {
	return func(c *connConfig) error {
		if tune == nil {
			return optionErrorf("WithSettingsFunc", "tune must not be nil")
		}
		tune(c.settings)
		return nil
	}
}

// WithLogger installs a logger, bypassing UseLogging and LogLevel entirely.
func WithLogger(logger *slog.Logger) ConnOption {
	return func(c *connConfig) error {
		if logger == nil {
			return optionErrorf("WithLogger", "logger must not be nil; omit the option to use the default")
		}
		c.logger = logger
		return nil
	}
}

// WithLogging turns on the built-in logger at the given level.
func WithLogging(level LogLevel) ConnOption {
	return func(c *connConfig) error {
		switch level {
		case DEBUG, INFO, WARNING, ERROR:
		default:
			return optionErrorf("WithLogging", "unknown log level %q", level)
		}
		c.settings.SetUseLogging(true)
		c.settings.SetLogLevel(level)
		return nil
	}
}

// WithMetrics turns on the ConnMetrics recording read by Connection.GetMetrics.
func WithMetrics() ConnOption {
	return func(c *connConfig) error {
		c.settings.SetUseMetrics(true)
		return nil
	}
}

// ------------
// Pooling
// ------------

// WithKeepAlive pools connections, keeping idlePerHost of them idle per host
// and allowing maxPerHost in total.
//
// Without this the transport closes every connection after one request, which
// also makes ConnectionWarmUp fail.
func WithKeepAlive(idlePerHost, maxPerHost uint64) ConnOption {
	return func(c *connConfig) error {
		if idlePerHost == 0 {
			return optionErrorf("WithKeepAlive", "idlePerHost must be positive")
		}
		if maxPerHost != 0 && maxPerHost < idlePerHost {
			// Idle connections are a subset of the total, so a smaller cap can
			// never be reached and would just close connections on return.
			return optionErrorf("WithKeepAlive", "maxPerHost (%d) is below idlePerHost (%d)", maxPerHost, idlePerHost)
		}

		c.settings.SetUseKeepAlive(true)
		c.settings.SetMaxIdleConnsPerHost(idlePerHost)
		c.settings.SetMaxConnsPerHost(maxPerHost)
		if c.settings.MaxIdleConns() < idlePerHost {
			c.settings.SetMaxIdleConns(idlePerHost)
		}
		return nil
	}
}

// WithMaxIdleConns sets the global idle ceiling across all hosts.
func WithMaxIdleConns(total uint64) ConnOption {
	return func(c *connConfig) error {
		if total == 0 {
			return optionErrorf("WithMaxIdleConns", "total must be positive")
		}
		c.settings.SetMaxIdleConns(total)
		return nil
	}
}

// ------------
// Timeouts and retries
// ------------

// WithTimeout sets the overall per-request budget.
func WithTimeout(timeout time.Duration) ConnOption {
	return func(c *connConfig) error {
		if timeout <= 0 {
			return optionErrorf("WithTimeout", "timeout must be positive, got %v", timeout)
		}
		c.settings.SetTimeout(timeout)
		return nil
	}
}

// WithHandshakeTimeout caps the TLS handshake.
func WithHandshakeTimeout(timeout time.Duration) ConnOption {
	return func(c *connConfig) error {
		if timeout <= 0 {
			return optionErrorf("WithHandshakeTimeout", "timeout must be positive, got %v", timeout)
		}
		c.settings.SetHandShakeTimeout(timeout)
		return nil
	}
}

// WithRetries replays a failed transport attempt up to attempts more times,
// pausing delay between them. Only transport failures are retried; a 5xx is a
// successful round trip and is never replayed.
func WithRetries(attempts uint64, delay time.Duration) ConnOption {
	return func(c *connConfig) error {
		if attempts == 0 {
			return optionErrorf("WithRetries", "attempts must be positive; omit the option for no retries")
		}
		if delay < 0 {
			return optionErrorf("WithRetries", "delay must not be negative, got %v", delay)
		}
		c.settings.SetMaxRetries(attempts)
		if delay > 0 {
			c.settings.SetRetryDelay(delay)
		}
		return nil
	}
}

// WithFailFast gives up as soon as a server stalls on the response headers,
// cancels the siblings of a failed batch request, and disables retries.
func WithFailFast() ConnOption {
	return func(c *connConfig) error {
		c.settings.SetUseFailFast(true)
		return nil
	}
}

// ------------
// Requests
// ------------

// WithAsync runs batch requests concurrently.
func WithAsync() ConnOption {
	return func(c *connConfig) error {
		c.settings.SetUseAsync(true)
		return nil
	}
}

// WithApiKey sends value in the header named by key, on every request that
// does not already set that header. The caller picks the header, so any scheme
// works:
//
//	WithApiKey("Authorization", "Bearer "+token)
//	WithApiKey("X-API-Key", key)
func WithApiKey(key, value string) ConnOption {
	return func(c *connConfig) error {
		header := strings.TrimSpace(key)
		if header == "" {
			return optionErrorf("WithApiKey", "header name must not be empty")
		}
		if err := validHeaderName(header); err != nil {
			return optionErrorf("WithApiKey", "%v", err)
		}
		if value == "" {
			// Sending an empty header is never what was meant, and it would
			// also mask a header the request sets for itself.
			return optionErrorf("WithApiKey", "value for %q must not be empty", header)
		}

		c.settings.SetUseApiKey(true)
		c.settings.SetApiKey(ApiKey{Header: header, Value: value})
		return nil
	}
}

// WithHeaders adds headers to every request that does not already set them.
func WithHeaders(header map[string]string) ConnOption {
	return func(c *connConfig) error {
		if len(header) == 0 {
			return optionErrorf("WithHeaders", "header must not be empty")
		}
		for name := range header {
			if err := validHeaderName(strings.TrimSpace(name)); err != nil {
				return optionErrorf("WithHeaders", "%v", err)
			}
		}
		c.settings.SetUseCachedHeaders(true)
		c.settings.SetCacheHeaders(header)
		return nil
	}
}

// WithRedirects follows up to max redirects. Redirects are off by default, so
// a 3xx otherwise arrives as an ordinary response.
func WithRedirects(max uint64) ConnOption {
	return func(c *connConfig) error {
		if max == 0 {
			return optionErrorf("WithRedirects", "max must be positive; omit the option to not follow redirects")
		}
		c.settings.SetAllowRedirects(true)
		c.settings.SetMaxRedirects(max)
		return nil
	}
}

// WithMaxResponseBodySize caps how many bytes Response.Bytes/Text/JSON buffer.
// A negative size means unlimited; Save and Stream are never capped.
func WithMaxResponseBodySize(size int64) ConnOption {
	return func(c *connConfig) error {
		if size == 0 {
			return optionErrorf("WithMaxResponseBodySize", "size must not be zero; use a negative size for unlimited")
		}
		c.settings.SetMaxResponseBodySize(size)
		return nil
	}
}

// ------------
// Cookies
// ------------

// WithCookies keeps cookies across requests in a jar created for this
// connection.
func WithCookies() ConnOption {
	return func(c *connConfig) error {
		c.settings.SetUseCookies(true)
		c.settings.SetCacheCookies(true)
		return nil
	}
}

// WithCookieJar keeps cookies in a jar you supply, which is how two
// connections share one session.
func WithCookieJar(jar http.CookieJar) ConnOption {
	return func(c *connConfig) error {
		if jar == nil {
			return optionErrorf("WithCookieJar", "jar must not be nil; use WithCookies to create one")
		}
		c.settings.SetUseCookies(true)
		c.settings.SetCookieJar(jar)
		return nil
	}
}

// ------------
// Transport
// ------------

// WithProxy routes requests through rawURL.
func WithProxy(rawURL string) ConnOption {
	return func(c *connConfig) error {
		trimmed := strings.TrimSpace(rawURL)
		if trimmed == "" {
			return optionErrorf("WithProxy", "url must not be empty; use WithProxyFromEnvironment instead")
		}

		parsed, err := url.Parse(trimmed)
		if err != nil {
			return optionErrorf("WithProxy", "cannot parse %q: %v", trimmed, err)
		}
		if parsed.Scheme == "" || parsed.Host == "" {
			return optionErrorf("WithProxy", "%q needs a scheme and a host, e.g. http://proxy:8080", trimmed)
		}

		c.settings.SetUseProxy(true)
		c.settings.SetProxyURL(trimmed)
		return nil
	}
}

// WithProxyFromEnvironment takes the proxy from HTTP_PROXY, HTTPS_PROXY and
// NO_PROXY.
func WithProxyFromEnvironment() ConnOption {
	return func(c *connConfig) error {
		c.settings.SetUseProxy(true)
		c.settings.SetProxyURL("")
		return nil
	}
}

// WithHTTP2 negotiates HTTP/2 in addition to whatever else is enabled.
func WithHTTP2() ConnOption {
	return func(c *connConfig) error {
		c.settings.SetUseHTTP2(true)
		return nil
	}
}

// WithProtocols sets the protocols to offer, in place of HttpVersion. Accepts
// "http/1.1", "h2" and "h2c" along with their common spellings.
func WithProtocols(names ...string) ConnOption {
	return func(c *connConfig) error {
		if len(names) == 0 {
			return optionErrorf("WithProtocols", "at least one protocol is required")
		}
		// protocolSet ignores names it does not know, so reject them here
		// rather than letting a typo silently fall back to HTTP/1.1.
		for _, name := range names {
			switch strings.ToLower(strings.TrimSpace(name)) {
			case "1", "1.0", "1.1", "http/1.0", "http/1.1", "http1",
				"2", "2.0", "h2", "http/2", "http2", "h2c":
			default:
				return optionErrorf("WithProtocols", "unknown protocol %q", name)
			}
		}
		c.settings.SetProtocols(names)
		return nil
	}
}

// WithInsecureTLS turns off TLS certificate verification.
//
// This is named as an opt-out on purpose: it makes every HTTPS request
// trivially interceptable, so it should only appear in tests and against hosts
// with a self-signed certificate you control.
func WithInsecureTLS() ConnOption {
	return func(c *connConfig) error {
		c.settings.SetSkipPeerVerification(true)
		return nil
	}
}

// WithCompressionMode sets who decodes compressed response bodies. See
// CompressionHandler for what each mode means.
// WithMiddleware registers middleware, in the order they should run. It can be
// used more than once and appends each time, so options assembled from several
// places do not overwrite each other.
//
//	conn, err := NewConnection(1, WithMiddleware(tracing(), timing(log)))
//
// The same stack is reachable afterwards through Connection.GetMiddleware, so
// nothing has to be known at construction time.
func WithMiddleware(middleware ...Middleware) ConnOption {
	return func(c *connConfig) error {
		var errs []error
		for i, mw := range middleware {
			if _, err := c.middleware.Add(mw); err != nil {
				errs = append(errs, optionErrorf("WithMiddleware", "middleware %d: %v", i, err))
			}
		}
		return errors.Join(errs...)
	}
}

func WithCompressionMode(mode CompressionMode) ConnOption {
	return func(c *connConfig) error {
		switch mode {
		case CompressionTransport, CompressionManual, CompressionOff:
		default:
			return optionErrorf("WithCompressionMode", "unknown compression mode %d", int(mode))
		}
		c.compressionMode = mode
		return nil
	}
}
