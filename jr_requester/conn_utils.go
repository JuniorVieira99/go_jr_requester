package jr_requester

import (
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"strings"
	"time"
)

// ------------
// Enums
// ------------

type ConnectionStatus int

const (
	Disconnected ConnectionStatus = iota
	Connected
	// ShutDown is terminal: the connection was killed by Connection.Shutdown
	// and will not serve another request.
	ShutDown
)

func (cs ConnectionStatus) String() string {
	switch cs {
	case Connected:
		return "Connected"
	case Disconnected:
		return "Disconnected"
	case ShutDown:
		return "ShutDown"
	default:
		return "Unknown"
	}
}

// Defaults for transport knobs that have no dedicated setting, and fallbacks
// for the settings whose zero value would otherwise mean "no limit".
const (
	defaultIdleConnTimeout       = 90 * time.Second
	defaultExpectContinueTimeout = 1 * time.Second
	defaultKeepAliveInterval     = 30 * time.Second
	defaultHandshakeTimeout      = 10 * time.Second
	defaultRequestTimeout        = 30 * time.Second
	defaultRetryDelay            = 200 * time.Millisecond
	defaultMaxResponseBodySize   = 10 << 20 // 10 MiB
)

type LogLevel string

const (
	INFO    LogLevel = "INFO"
	DEBUG   LogLevel = "DEBUG"
	WARNING LogLevel = "WARNING"
	ERROR   LogLevel = "ERROR"
)

type HTTPMethod string

const (
	GET     HTTPMethod = "GET"
	POST    HTTPMethod = "POST"
	PUT     HTTPMethod = "PUT"
	DELETE  HTTPMethod = "DELETE"
	PATCH   HTTPMethod = "PATCH"
	HEAD    HTTPMethod = "HEAD"
	OPTIONS HTTPMethod = "OPTIONS"
)

// ------------
// Structs
// ------------

// ApiKey is the header an API key is sent in, and its value. The caller
// chooses both, so any scheme works:
//
//	ApiKey{Header: "Authorization", Value: "Bearer " + token}
//	ApiKey{Header: "X-API-Key", Value: key}
//
// The zero value sends nothing.
type ApiKey struct {
	Header string `json:"header"`
	Value  string `json:"value"`
}

// IsZero reports whether the pair is unset, which is when either half is
// missing: a header with no value and a value with no header are both useless.
func (k ApiKey) IsZero() bool {
	return strings.TrimSpace(k.Header) == "" || k.Value == ""
}

// String redacts the value so an ApiKey cannot be logged by accident.
func (k ApiKey) String() string {
	if k.IsZero() {
		return "ApiKey{unset}"
	}
	return fmt.Sprintf("ApiKey{header: %q, value: [REDACTED]}", k.Header)
}

// BatchResult pairs a request with its outcome. Response is nil when Err is
// set. Results are always returned in the same order as the input.
//
// Every non-nil Response owns a connection until it is closed. CloseResults is
// the easy way to release a whole batch at once.
type BatchResult struct {
	Request  *http.Request
	Response *Response
	Err      error
}

// ------------
// Helpers
// ------------

// newDefaultLogger honours UseLogging and LogLevel.
func newDefaultLogger(settings *ConnSettings) *slog.Logger {
	if !settings.useLogging {
		return slog.New(slog.DiscardHandler)
	}
	var level slog.Level
	switch settings.logLevel {
	case DEBUG:
		level = slog.LevelDebug
	case WARNING:
		level = slog.LevelWarn
	case ERROR:
		level = slog.LevelError
	case INFO:
		fallthrough
	default:
		level = slog.LevelInfo
	}
	return slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: level}))
}

// validHeaderName checks name is a valid HTTP field name, which RFC 9110
// defines as a non-empty token. Without this a name containing a space or a
// colon would produce a header no server can parse.
func validHeaderName(name string) error {
	if name == "" {
		return errors.New("header name must not be empty")
	}
	for _, r := range name {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9':
		case strings.ContainsRune("!#$%&'*+-.^_`|~", r):
		default:
			return fmt.Errorf("header name %q contains an invalid character %q", name, r)
		}
	}
	return nil
}

// assignUint stores value into target if it is any non-negative integer. It
// accepts float64 so that JSON-decoded maps work, but rejects fractions.
func assignUint(value any, target *uint64) bool {
	switch v := value.(type) {
	case uint64:
		*target = v
	case uint:
		*target = uint64(v)
	case uint32:
		*target = uint64(v)
	case uint16:
		*target = uint64(v)
	case uint8:
		*target = uint64(v)
	case int:
		if v < 0 {
			return false
		}
		*target = uint64(v)
	case int64:
		if v < 0 {
			return false
		}
		*target = uint64(v)
	case int32:
		if v < 0 {
			return false
		}
		*target = uint64(v)
	case float64:
		if v < 0 || v != float64(uint64(v)) {
			return false
		}
		*target = uint64(v)
	default:
		return false
	}
	return true
}

// assignInt stores value into target from any signed integer, or a float64
// with no fractional part so JSON-decoded maps work. Negative values are
// allowed: some settings use them as an explicit "no limit".
func assignInt(value any, target *int64) bool {
	switch v := value.(type) {
	case int64:
		*target = v
	case int:
		*target = int64(v)
	case int32:
		*target = int64(v)
	case int16:
		*target = int64(v)
	case uint64:
		*target = int64(v)
	case uint:
		*target = int64(v)
	case uint32:
		*target = int64(v)
	case float64:
		if v != float64(int64(v)) {
			return false
		}
		*target = int64(v)
	default:
		return false
	}
	return true
}

// assignDuration stores value into target from a time.Duration or a string in
// time.ParseDuration form ("30s", "1m500ms").
func assignDuration(value any, target *time.Duration) bool {
	switch v := value.(type) {
	case time.Duration:
		*target = v
	case string:
		parsed, err := time.ParseDuration(v)
		if err != nil {
			return false
		}
		*target = parsed
	default:
		return false
	}
	return true
}

func assignBool(value any, target *bool) bool {
	v, ok := value.(bool)
	if ok {
		*target = v
	}
	return ok
}

func assignString(value any, target *string) bool {
	v, ok := value.(string)
	if ok {
		*target = v
	}
	return ok
}

// ErrorFormat builds an error naming the offending option.
func ErrorFormat(e error, message string, format string, args ...any) error {
	return fmt.Errorf("%w: %s: %s", e, message, fmt.Sprintf(format, args...))
}
