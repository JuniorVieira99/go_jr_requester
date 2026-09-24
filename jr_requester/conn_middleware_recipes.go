package jr_requester

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"net/http"
	"slices"
	"strings"
	"time"
)

var (
	ErrAuthorizationHeaderMissing = errors.New("missing authorization header")
	ErrAuthorizationFailed        = errors.New("authorization failed")
	ErrMissingHeader              = errors.New("missing required header")
	ErrMissingQueryParam          = errors.New("missing required query parameter")
	ErrNilHttpRequest             = errors.New("nil http request")
	ErrNilRoundTripFunc           = errors.New("nil round trip function")
	// ErrInvalidRecipe is returned on every request by a recipe that was built
	// with an argument it cannot work with, such as an empty expected value.
	ErrInvalidRecipe = errors.New("invalid middleware recipe")
	// ErrHostNotAllowed is returned when a request targets a host outside the
	// allow-list given to MiddlewareAllowedHosts.
	ErrHostNotAllowed = errors.New("host not allowed")
	// ErrInsecureScheme is returned by MiddlewareRequireHTTPS for a plain-HTTP
	// request.
	ErrInsecureScheme = errors.New("insecure scheme: https required")
)

// ------------
// Helpers
// ------------

// reject fails a request without sending it. A RoundTripper must close the
// request body even on error, and http.Client does not do it for us, so a
// middleware that refuses a request has to.
func reject(req *http.Request, err error) (*http.Response, error) {
	if req != nil && req.Body != nil {
		req.Body.Close()
	}
	return nil, err
}

// checkHop guards against being called outside a chain.
func checkHop(req *http.Request, next RoundTripFunc) error {
	if req == nil {
		return ErrNilHttpRequest
	}
	if next == nil {
		return ErrNilRoundTripFunc
	}
	return nil
}

// failing is what a recipe returns when its arguments are unusable: it rejects
// every request with err, so the mistake shows up on the first call rather than
// as a guard that silently lets everything through.
func failing(err error) Middleware {
	return func(req *http.Request, next RoundTripFunc) (*http.Response, error) {
		return reject(req, err)
	}
}

// cloneExpected deep-copies a map of expected values, so a caller editing
// their map later neither races with requests nor changes the rule, and checks
// that every expected value is non-blank.
func cloneExpected(what string, expected map[string][]string) (map[string][]string, error) {
	cloned := make(map[string][]string, len(expected))
	for key, values := range expected {
		if len(values) == 0 {
			return nil, fmt.Errorf("%w: %s %q has no expected values", ErrInvalidRecipe, what, key)
		}
		for _, v := range values {
			if strings.TrimSpace(v) == "" {
				return nil, fmt.Errorf("%w: %s %q has an empty expected value", ErrInvalidRecipe, what, key)
			}
		}
		cloned[key] = slices.Clone(values)
	}
	return cloned, nil
}

// ------------
// Guards
// ------------

// MiddlewareSecureToken is a middleware that checks for a specific authorization token in the request header.
//
// The API key set with WithApiKey is applied downstream of the middleware
// stack, so this guard does not see it: use it for tokens the caller or an
// earlier middleware sets on the request.
func MiddlewareSecureToken(passKey string, passToken string) Middleware {
	if strings.TrimSpace(passKey) == "" || strings.TrimSpace(passToken) == "" {
		return failing(fmt.Errorf("%w: MiddlewareSecureToken needs a header name and a non-empty token", ErrInvalidRecipe))
	}
	return func(req *http.Request, next RoundTripFunc) (*http.Response, error) {
		if err := checkHop(req, next); err != nil {
			return reject(req, err)
		}
		values := req.Header.Values(passKey)
		if len(values) == 0 {
			return reject(req, ErrAuthorizationHeaderMissing)
		}
		if strings.TrimSpace(values[0]) != passToken {
			return reject(req, ErrAuthorizationFailed)
		}
		return next(req)
	}
}

// MiddlewareSecureHeaders is a middleware that checks for the presence of specific headers in the request.
func MiddlewareSecureHeaders(headers []string) Middleware {
	headers = slices.Clone(headers)
	return func(req *http.Request, next RoundTripFunc) (*http.Response, error) {
		if err := checkHop(req, next); err != nil {
			return reject(req, err)
		}
		for _, h := range headers {
			values := req.Header.Values(h)
			if len(values) == 0 || strings.TrimSpace(values[0]) == "" {
				return reject(req, fmt.Errorf("%w: header %q is missing or empty", ErrMissingHeader, h))
			}
		}
		return next(req)
	}
}

// MiddlewareSecureHeadersAndValues is a middleware that checks for specific headers and their expected values in the request.
// Header names are matched case-insensitively; values must match exactly.
func MiddlewareSecureHeadersAndValues(headers map[string][]string) Middleware {
	expected, err := cloneExpected("header", headers)
	if err != nil {
		return failing(err)
	}
	keys := slices.Sorted(maps.Keys(expected))
	return func(req *http.Request, next RoundTripFunc) (*http.Response, error) {
		if err := checkHop(req, next); err != nil {
			return reject(req, err)
		}
		for _, key := range keys {
			got := req.Header.Values(key)
			if len(got) == 0 {
				return reject(req, fmt.Errorf("%w: header %q is missing", ErrMissingHeader, key))
			}
			for _, want := range expected[key] {
				if !slices.Contains(got, want) {
					return reject(req, fmt.Errorf("%w: header %q value %q is missing", ErrMissingHeader, key, want))
				}
			}
		}
		return next(req)
	}
}

// MiddlewareSecureQueryParams is a middleware that checks for the presence of specific query parameters in the request URL.
func MiddlewareSecureQueryParams(params []string) Middleware {
	params = slices.Clone(params)
	return func(req *http.Request, next RoundTripFunc) (*http.Response, error) {
		if err := checkHop(req, next); err != nil {
			return reject(req, err)
		}
		if req.URL == nil {
			return reject(req, fmt.Errorf("%w: request has no URL", ErrMissingQueryParam))
		}
		query := req.URL.Query()
		for _, p := range params {
			values := query[p]
			if len(values) == 0 || strings.TrimSpace(values[0]) == "" {
				return reject(req, fmt.Errorf("%w: query parameter %q is missing or empty", ErrMissingQueryParam, p))
			}
		}
		return next(req)
	}
}

// MiddlewareSecureQueryParamsAndValues is a middleware that checks for specific query parameters and their expected values in the request URL.
// It ensures that each specified query parameter exists and has the expected values.
//
// map[string][]string: a map where the key is the query parameter name and the value is a slice of expected values.
//
//	var myQueryParams = map[string][]string{
//	    "param1": {"value1"},
//	    "param2": {"value2"},
//	}
func MiddlewareSecureQueryParamsAndValues(params map[string][]string) Middleware {
	expected, err := cloneExpected("query parameter", params)
	if err != nil {
		return failing(err)
	}
	keys := slices.Sorted(maps.Keys(expected))
	return func(req *http.Request, next RoundTripFunc) (*http.Response, error) {
		if err := checkHop(req, next); err != nil {
			return reject(req, err)
		}
		if req.URL == nil {
			return reject(req, fmt.Errorf("%w: request has no URL", ErrMissingQueryParam))
		}
		query := req.URL.Query()
		for _, key := range keys {
			got := query[key]
			if len(got) == 0 {
				return reject(req, fmt.Errorf("%w: query parameter %q is missing", ErrMissingQueryParam, key))
			}
			for _, want := range expected[key] {
				if !slices.Contains(got, want) {
					return reject(req, fmt.Errorf("%w: query parameter %q value %q is missing", ErrMissingQueryParam, key, want))
				}
			}
		}
		return next(req)
	}
}

// MiddlewareAllowedHosts refuses any request whose host is not in the list,
// which keeps a connection that builds URLs from untrusted input from being
// pointed somewhere else. Hosts are compared case-insensitively and without
// the port.
func MiddlewareAllowedHosts(hosts ...string) Middleware {
	allowed := make(map[string]struct{}, len(hosts))
	for _, h := range hosts {
		allowed[strings.ToLower(strings.TrimSpace(h))] = struct{}{}
	}
	return func(req *http.Request, next RoundTripFunc) (*http.Response, error) {
		if err := checkHop(req, next); err != nil {
			return reject(req, err)
		}
		if req.URL == nil {
			return reject(req, fmt.Errorf("%w: request has no URL", ErrHostNotAllowed))
		}
		host := strings.ToLower(req.URL.Hostname())
		if _, ok := allowed[host]; !ok {
			return reject(req, fmt.Errorf("%w: %q", ErrHostNotAllowed, host))
		}
		return next(req)
	}
}

// MiddlewareRequireHTTPS refuses plain-HTTP requests, so credentials set on
// the request are never sent in the clear.
func MiddlewareRequireHTTPS() Middleware {
	return func(req *http.Request, next RoundTripFunc) (*http.Response, error) {
		if err := checkHop(req, next); err != nil {
			return reject(req, err)
		}
		if req.URL == nil || !strings.EqualFold(req.URL.Scheme, "https") {
			return reject(req, ErrInsecureScheme)
		}
		return next(req)
	}
}

// ------------
// Decorators
// ------------

// MiddlewareSetHeaders sets every header in the map on the request,
// overwriting any value already there. The map is copied, so editing it
// afterwards does not change the middleware.
func MiddlewareSetHeaders(headers map[string]string) Middleware {
	headers = maps.Clone(headers)
	return func(req *http.Request, next RoundTripFunc) (*http.Response, error) {
		if err := checkHop(req, next); err != nil {
			return reject(req, err)
		}
		if req.Header == nil {
			req.Header = make(http.Header)
		}
		for key, value := range headers {
			req.Header.Set(key, value)
		}
		return next(req)
	}
}

// MiddlewareBearerToken sets "Authorization: Bearer <token>" from source on
// every request. Unlike WithApiKey the token is fetched per request, so source
// can refresh an expiring token; it receives the request's context.
func MiddlewareBearerToken(source func(ctx context.Context) (string, error)) Middleware {
	if source == nil {
		return failing(fmt.Errorf("%w: MiddlewareBearerToken needs a token source", ErrInvalidRecipe))
	}
	return func(req *http.Request, next RoundTripFunc) (*http.Response, error) {
		if err := checkHop(req, next); err != nil {
			return reject(req, err)
		}
		token, err := source(req.Context())
		if err != nil {
			return reject(req, fmt.Errorf("%w: token source: %w", ErrAuthorizationFailed, err))
		}
		if strings.TrimSpace(token) == "" {
			return reject(req, fmt.Errorf("%w: token source returned an empty token", ErrAuthorizationHeaderMissing))
		}
		if req.Header == nil {
			req.Header = make(http.Header)
		}
		req.Header.Set("Authorization", "Bearer "+token)
		return next(req)
	}
}

// MiddlewareRequestID tags each request with a random id in header, unless the
// caller already set one. An empty header defaults to X-Request-Id.
func MiddlewareRequestID(header string) Middleware {
	if header == "" {
		header = "X-Request-Id"
	}
	return func(req *http.Request, next RoundTripFunc) (*http.Response, error) {
		if err := checkHop(req, next); err != nil {
			return reject(req, err)
		}
		if req.Header == nil {
			req.Header = make(http.Header)
		}
		if req.Header.Get(header) == "" {
			var id [16]byte
			rand.Read(id[:])
			req.Header.Set(header, hex.EncodeToString(id[:]))
		}
		return next(req)
	}
}

// MiddlewareTimeout bounds the whole logical request, every retry and the body
// read included, to d. The deadline is released when the caller closes the
// body, not when the middleware returns, since cancelling any earlier would
// cut off a body that has not been read yet.
func MiddlewareTimeout(d time.Duration) Middleware {
	if d <= 0 {
		return failing(fmt.Errorf("%w: MiddlewareTimeout needs a positive duration, got %v", ErrInvalidRecipe, d))
	}
	return func(req *http.Request, next RoundTripFunc) (*http.Response, error) {
		if err := checkHop(req, next); err != nil {
			return reject(req, err)
		}
		ctx, cancel := context.WithTimeout(req.Context(), d)
		resp, err := next(req.WithContext(ctx))
		if err != nil || resp == nil || resp.Body == nil {
			cancel()
			return resp, err
		}
		resp.Body = &releaseOnClose{ReadCloser: resp.Body, release: cancel}
		return resp, nil
	}
}

// ------------
// Observers
// ------------

// MiddlewareLogger logs one line per logical request: method, redacted URL,
// status and duration. Headers are never logged, and the API key is applied
// downstream, so neither can leak into the log.
func MiddlewareLogger(logger *slog.Logger) Middleware {
	if logger == nil {
		logger = slog.Default()
	}
	return func(req *http.Request, next RoundTripFunc) (*http.Response, error) {
		if err := checkHop(req, next); err != nil {
			return reject(req, err)
		}
		start := time.Now()
		resp, err := next(req)

		target := ""
		if req.URL != nil {
			target = req.URL.Redacted()
		}
		attrs := []any{"method", req.Method, "url", target, "took", time.Since(start)}
		switch {
		case err != nil:
			logger.ErrorContext(req.Context(), "request failed", append(attrs, "error", err)...)
		case resp != nil:
			logger.InfoContext(req.Context(), "request", append(attrs, "status", resp.StatusCode)...)
		}
		return resp, err
	}
}

// MiddlewarePrint prints one line per logical request: method, URL, and duration.
// It prints to the standard output. Headers and API keys are not logged.
//
// Prints:
//
// Method: <HTTP method>
// URL: <request URL>
// Duration: <time taken>
// Response: <HTTP status code> (if successful)
// Request failed: <error> (if failed)
func MiddlewarePrint() Middleware {
	return func(req *http.Request, next RoundTripFunc) (*http.Response, error) {
		if err := checkHop(req, next); err != nil {
			return reject(req, err)
		}
		timestamp := time.Now()
		resp, err := next(req)

		var stringToPrint strings.Builder
		if err != nil {
			stringToPrint.WriteString(fmt.Sprintf("Method: %s\nURL: %s\nDuration: %s\n", req.Method, req.URL, time.Since(timestamp)))
			stringToPrint.WriteString(fmt.Sprintf("Request failed: %v\n", err))
		} else if resp != nil {
			stringToPrint.WriteString(fmt.Sprintf("Method: %s\nURL: %s\nDuration: %s\n", req.Method, req.URL, time.Since(timestamp)))
			stringToPrint.WriteString(fmt.Sprintf("Response: %d\nDuration: %s\n", resp.StatusCode, time.Since(timestamp)))
		}
		fmt.Print(stringToPrint.String())
		return resp, err
	}
}
