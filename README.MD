# jr_requester

A configurable HTTP client for Go. It wraps `net/http` with explicit control
over connection pooling, retries, redirects, proxies and TLS, adds concurrent
batch requests, and returns a response type that is hard to leak.

- **Functional options that validate** — `WithKeepAlive`, `WithRetries`,
  `WithProxy`, `WithApiKey`, `WithCompressionMode`, `WithProtocols` all check 
  their arguments and return an error if they are invalid. `NewConnection` 
  reports all failing options at once, not just the first.
- **Responses that clean up after themselves** — the body is buffered once,
  `Close()` drains it so the connection goes back into the pool, and every
  method is safe on a nil response.
- **Batch requests** — concurrent or sequential, always returned in input
  order, with per-request errors instead of a single aggregate.
- **Retries** — transport failures only, applied in one place at the
  transport layer, with a fixed delay between attempts.
- **Optional metrics** — request counts and connection reuse sampled through
  `net/http/httptrace`.
- **A kill switch** — `Shutdown()` cancels every in-flight request and retires
  the connection, without threading a context through every call site.
- **Middleware** — wrap every request with logging, tracing, auth or a cache,
  editable on a live connection, with built-in recipes for header and query
  guards, host allow-lists, bearer tokens, request ids, timeouts and logging.
- **No third-party dependencies** — standard library only.

---

## Contents

- [Requirements](#requirements)
- [Installation](#installation)
- [Quick start](#quick-start)
- [Core concepts](#core-concepts)
- [API reference](#api-reference)
  - [Connection](#connection)
  - [RestHandler](#resthandler)
  - [Response](#response)
  - [Batch results](#batch-results)
  - [Options](#options)
  - [ConnSettings](#connsettings)
  - [ConnMetrics](#connmetrics)
  - [Compression](#compression)
  - [Middleware](#middleware)
  - [Errors and enums](#errors-and-enums)
- [Recipes](#recipes)
- [Tuning the pool](#tuning-the-pool)
- [Gotchas](#gotchas)
- [Testing](#testing)
- [Known limitations](#known-limitations)

---

## Requirements

Go 1.26.5 or newer, as pinned by `go.mod`. The library uses
`sync.WaitGroup.Go` (1.25), `slog.DiscardHandler` (1.24) and
`http.Transport.Protocols` (1.24).

## Installation

The module is `jr_requester`, so the package imports as:

```go
import requester "jr_requester/jr_requester"
```

It is not published yet. To use it from another module, vendor it or point at
a local checkout:

```sh
go mod edit -replace jr_requester=../JrRequester
```

## Quick start

```go
package main

import (
    "context"
    "fmt"

    requester "jr_requester/jr_requester"
)

type Post struct {
    ID     int    `json:"id"`
    UserID int    `json:"userId"`
    Title  string `json:"title"`
}

func main() {
    // No options at all = defaults.
    conn, err := requester.NewConnection(1)
    if err != nil {
        panic(err) // an option rejected its argument
    }
    // Teardown: cancels anything in flight and retires the connection.
    // Use conn.Close() instead to only drop idle connections and keep going.
    defer conn.Shutdown()

    // A handler with all verbs, plus batch methods. It is safe to share across goroutines.
    rest := conn.GetRestHandler()

    resp, err := rest.Get(context.Background(), "https://api.example.com/posts/1", nil)
    if err != nil {
        panic(err) // the request never reached a response
    }
    defer resp.Close() // returns the connection to the pool

    if err := resp.ErrorForStatus(); err != nil {
        panic(err) // 4xx/5xx, with the start of the body quoted
    }

    var post Post
    if err := resp.JSON(&post); err != nil {
        panic(err)
    }
    fmt.Printf("%+v\n", post)
}
```

A configured connection looks like this:

```go
conn, err := requester.NewConnection(1,
    requester.WithKeepAlive(16, 32),                  // pool connections
    requester.WithAsync(),                            // run batches concurrently
    requester.WithMetrics(),                          // record request and pool metrics
    requester.WithRetries(2, 250*time.Millisecond),   // retry transport failures
    requester.WithApiKey("X-API-Key", os.Getenv("API_KEY")),
)
if err != nil {
    return err
}
defer conn.Shutdown()
```

---

## Core concepts

### A `Connection` is a pool, not a socket

Despite the name, a `Connection` is not one TCP connection. It wraps one
`http.Client` and one `http.Transport`, and **the transport is the connection
pool**. Create one `Connection` per configuration — per API, per tenant, per
auth identity — and share it across goroutines. Both the client and the
transport are safe for concurrent use.

Creating a `Connection` per request is the one thing that breaks everything:
each gets a private pool of one, nothing is ever reused, and file descriptors
pile up.

### Responses own a connection until closed

A connection returns to the pool only once its response body is drained and
closed. `Response.Close()` does both, is idempotent, and is safe on a nil
receiver — so `defer resp.Close()` immediately after the call is always
correct, even on the error path.

`Bytes()`, `Text()` and `JSON()` close the body themselves after buffering it,
so a `defer resp.Close()` alongside them is redundant but harmless.

### Transport errors vs. HTTP errors

`err != nil` from a request means the client **never got a response** — DNS
failure, refused connection, TLS failure, timeout, cancelled context. A 404 or
a 500 is a *successful* round trip; the status lives on the response. Use
`resp.ErrorForStatus()` to turn a non-2xx into an error value.

### Two layers of API

| Layer | Entry points | Takes | Returns |
| --- | --- | --- | --- |
| Low level | `Connection.DoRequest`, `Connection.DoBatchRequests` | `*http.Request` | `*http.Response`, `[]BatchResult` |
| Convenience | `Connection.DoManual*`, `RestHandler` | URL, method, headers, body | `*Response`, `[]BatchResult` |

Use the low-level layer when you have built an `*http.Request` yourself; use
`RestHandler` for everything else.

---

## API reference

### Connection

```go
func NewConnection(id uint64, opts ...ConnOption) (*Connection, error)
```

With no options it returns a working connection on the defaults. Options are
applied in order and the later one wins, so a broad option can be narrowed by a
specific one after it. A `nil` option is a no-op, so a conditionally assembled
option slice needs no filtering.

**Options can fail**, and every failing one is reported rather than just the
first, so a single round of fixes clears them all. Errors match
`errors.Is(err, ErrInvalidOption)`, or `ErrSettingsNotFirst` for an
out-of-order `WithSettings`. A failed construction returns a nil `*Connection`.

The settings are copied on the way in, so retuning what you passed to
`WithSettings` afterwards cannot change a connection that is already built.

| Method | Description |
| --- | --- |
| `DoRequest(ctx, *http.Request) (*http.Response, error)` | Low-level single request. **You** must drain and close the body. |
| `DoBatchRequests(ctx, []*http.Request) []BatchResult` | Runs a batch, concurrently if `UseAsync`. |
| `DoManualRequest(ctx, method, url, header, body) (*Response, error)` | Builds and sends one request. |
| `DoManualBatchRequests(ctx, method, urls, header, body) []BatchResult` | Same body to every URL, buffered once. |
| `DoManualBatchRequestsWithBodies(ctx, method, urls, header, bodies) []BatchResult` | Pairs `urls[i]` with `bodies[i]`. |
| `ConnectionWarmUp(ctx, host, n) error` | Pre-opens `n` pooled connections. |
| `Close()` | Drops pooled connections; the connection stays usable. |
| `Shutdown()` | Kill switch: cancels every in-flight request and retires the connection for good. |
| `IsShutdown() bool` | Whether `Shutdown` has been called. |
| `InFlight() int` | Requests whose body is still open — what `Shutdown` would cancel now. |
| `Status() ConnectionStatus` | `Disconnected`, `Connected`, `Idle` or `ShutDown`. |
| `GetRestHandler() *RestHandler` | The verb helpers. |
| `GetCompressionHandler() *CompressionHandler` | The compression policy and gzip/zlib helpers. |
| `GetMiddleware() *MiddlewareStack` | The live middleware chain. |
| `GetCopySettings() *ConnSettings` | A **copy** — editing it does not reconfigure the live transport. |
| `GetMetrics() *ConnMetrics` | A snapshot, or nil when metrics are off. |
| `GetClient() *http.Client`, `GetTransport() *http.Transport` | Escape hatches. The client has no `Timeout` of its own, because `DoRequest` applies it through the request context, and requests sent through it skip the in-flight tracking `Shutdown` uses. |

`Close()` is not a teardown: it calls `CloseIdleConnections` and marks the
status disconnected. In-flight requests are unaffected and later requests
simply dial again.

`Shutdown()` is the teardown. Every request runs under a cancel the connection
keeps hold of, so `Shutdown` can abort the ones already running: each fails with
a `context.Canceled` error on its own goroutine. It then drops the pooled
connections and marks the connection `ShutDown`, after which every request —
including the batch and `RestHandler` paths — fails with `ErrConnectionShutdown`
instead of dialling again. It is idempotent, and unlike `Close` it cannot be
undone.

```go
conn, err := requester.NewConnection(1)
if err != nil {
    return err
}
defer conn.Shutdown()
```

`Shutdown` returns once the cancellations are delivered, not once the callers
have noticed them. To wait for the work to unwind, join your own goroutines
afterwards.

A request holds its slot until its **response body is closed**, because the body
is read under the request's context. That is the same `Close()` the pool already
requires, so nothing new is owed — but a leaked body shows up in `InFlight()`.

#### Options

| Option | Sets |
| --- | --- |
| `WithSettings(s)` | Seeds from an existing `ConnSettings`. **Must be first.** |
| `WithSettingsFunc(f)` | Escape hatch: runs `f` against the settings being built. |
| `WithLogger(l)` | Installs a logger, bypassing `UseLogging`/`LogLevel`. |
| `WithLogging(level)` | `UseLogging` + `LogLevel`. |
| `WithMetrics()` | `UseMetrics`. |
| `WithKeepAlive(idlePerHost, maxPerHost)` | `UseKeepAlive` + both per-host caps, raising the global idle ceiling to match. `maxPerHost` of 0 means unlimited. |
| `WithMaxIdleConns(total)` | `MaxIdleConns`. |
| `WithTimeout(d)` | `Timeout`. |
| `WithHandshakeTimeout(d)` | `HandShakeTimeout`. |
| `WithRetries(attempts, delay)` | `MaxRetries` + `RetryDelay`. |
| `WithFailFast()` | `UseFailFast`. |
| `WithAsync()` | `UseAsync`. |
| `WithApiKey(header, value)` | `UseApiKey` + `ApiKey`; the caller picks the header. |
| `WithHeaders(m)` | `UseCachedHeaders` + `CacheHeaders`. |
| `WithRedirects(max)` | `AllowRedirects` + `MaxRedirects`. |
| `WithMaxResponseBodySize(n)` | `MaxResponseBodySize`; negative means unlimited. |
| `WithCookies()` | `UseCookies` + `CacheCookies`. |
| `WithCookieJar(jar)` | `UseCookies` + `CookieJar`, for sharing a session. |
| `WithProxy(url)` | `UseProxy` + `ProxyURL`, validating the URL. |
| `WithProxyFromEnvironment()` | `UseProxy` with the environment's proxy. |
| `WithHTTP2()` | `UseHTTP2`. |
| `WithProtocols(names...)` | `Protocols`, rejecting names it does not know. |
| `WithInsecureTLS()` | `SkipPeerVerification`. |
| `WithCompressionMode(mode)` | The handler's compression mode. |
| `WithMiddleware(mw...)` | Registers middleware, in order. Appends, so it can be used more than once. |

Flag-style options take no argument — `WithHTTP2()`, not
`WithHTTP2(true)` — so a call site never reads as a bare `true` or `false` you
have to look up. The one security-relevant switch is named for what it does:
`WithInsecureTLS()`.

`WithSettings` replaces the whole settings value, so anything before it would
be silently discarded. Rather than let that happen quietly it is rejected with
`ErrSettingsNotFirst`:

```go
settings, _ := requester.NewConnSettingsFromMap(fromConfigFile)

conn, err := requester.NewConnection(1,
    requester.WithSettings(settings), // first
    requester.WithHTTP2(),            // then override
)
```

For anything without a dedicated option, reach through `WithSettingsFunc`:

```go
requester.WithSettingsFunc(func(s *requester.ConnSettings) {
    s.SetHttpVersion("2")
})
```


### RestHandler

Obtained with `conn.GetRestHandler()`. Every method takes `ctx` first and
accepts `nil` for `header`.

```go
Get(ctx, url, header)            (*Response, error)
Delete(ctx, url, header)         (*Response, error)
Head(ctx, url, header)           (*Response, error)
Options(ctx, url, header)        (*Response, error)
Post(ctx, url, header, body)     (*Response, error)
Put(ctx, url, header, body)      (*Response, error)
Patch(ctx, url, header, body)    (*Response, error)
```

Batch equivalents return `[]BatchResult`:

```go
BatchGet(ctx, urls, header)              BatchDelete(ctx, urls, header)
BatchHead(ctx, urls, header)             BatchOptions(ctx, urls, header)
BatchPost(ctx, urls, header, bodies)     BatchPut(ctx, urls, header, bodies)
BatchPatch(ctx, urls, header, bodies)
```

The body-carrying batch methods take `[]io.Reader`, one per URL. A shorter
slice leaves the trailing requests without a body.

### Response

Wraps `*http.Response`. `Bytes`, `Text` and `JSON` read the body at most once
and cache it, so they can be called in any order and repeatedly. `Stream` and
`Save` hand the body over instead of caching it, and are single-use — see
below.

**Reading the body**

| Method | Description |
| --- | --- |
| `Bytes() ([]byte, error)` | Buffers, caches and returns the body. Do not modify the slice. |
| `Text() (string, error)` | The body as a string. |
| `JSON(v any) error` | Decodes into `v`; errors mention the status and content type. |
| `Stream() (io.ReadCloser, error)` | Unbuffered body for large payloads. **Caller closes it.** |
| `Save(w io.Writer) (int64, error)` | Streams the body into `w`. Ignores the size cap. |
| `Close() error` | Drains up to 64 KiB and closes. Idempotent, nil-safe. |
| `Discard() error` | Drains up to `MaxResponseBodySize` and closes, so a large unread body still returns its connection. A body declared over the limit is closed unread. Idempotent, nil-safe. |

`Bytes`, `Text` and `JSON` are capped by `MaxResponseBodySize` (10 MiB by
default) and return `ErrBodyTooLarge` past it. `Stream` and `Save` are never
capped, because nothing is held in memory.

Calling `Stream()` twice, or `Bytes()` after `Stream()`, returns
`ErrBodyConsumed`. Calling `Stream()` *after* buffering replays the cached copy.

**Metadata**

| Method | Description |
| --- | --- |
| `StatusCode() int` | 0 on a nil response. |
| `Status() string` | e.g. `"404 Not Found"`. |
| `Proto() string` | e.g. `"HTTP/2.0"`. |
| `Header() http.Header` | Never nil. |
| `ContentType() string` | Media type only — `"application/json"`, not `"application/json; charset=utf-8"`. |
| `ContentLength() int64` | `-1` when unknown. |
| `Cookies() []*http.Cookie` | From `Set-Cookie`. |
| `Request() *http.Request` | The final request, after redirects. |
| `URL() string` | Final URL, with credentials redacted. |
| `Raw() *http.Response` | The wrapped response. |
| `String() string` | One-line summary; does not touch the body. |

**Status classification**

`IsSuccess()` (2xx), `IsRedirect()` (3xx), `IsClientError()` (4xx),
`IsServerError()` (5xx), `IsError()` (>= 400).

`ErrorForStatus() error` returns nil for 2xx, otherwise a `*StatusError`:

```go
type StatusError struct {
    StatusCode int
    Status     string
    URL        string
    Snippet    string // first 512 bytes of the body, whitespace collapsed
}
```

The snippet is the reason this exists — when an API rejects a call, the
explanation is in the body, not the status line.

### Batch results

```go
type BatchResult struct {
    Request  *http.Request
    Response *Response
    Err      error
}
```

`[]BatchResult` is always the same length as the input and in the same order.
Every entry is populated: a request that failed to build, was nil, or was
skipped carries an `Err` rather than being silently empty.

| Helper | Description |
| --- | --- |
| `CloseResults(results)` | Closes every response. `defer` it right after the call. |
| `FirstError(results) error` | The first non-nil `Err`, or nil. |
| `Errors(results) []error` | Every non-nil `Err`, in order. |

Both prefix the message with `request <i>:` and wrap the original, so
`errors.Is` still works. They report whatever is in `Err` — a transport
failure, a request that failed to build, a nil entry, or `ErrBatchAborted` —
and none of them look at status codes. Use `ErrorForStatus` per response for
that.

```go
results := rest.BatchGet(ctx, urls, nil)
defer requester.CloseResults(results)

if err := requester.FirstError(results); err != nil {
    return err
}
for i, r := range results {
    if err := r.Response.ErrorForStatus(); err != nil {
        return fmt.Errorf("request %d: %w", i, err)
    }
    body, err := r.Response.Text()
    // ...
}
```

`UseAsync` selects concurrent or sequential execution. Concurrency is bounded
by `MaxConnsPerHost` so a 10,000-URL batch does not park 10,000 goroutines.
`MaxConnsPerHost` of 0 means unlimited, and there the bound is the batch size.
With `UseFailFast`, the first failure cancels the siblings and the untouched
entries are marked `ErrBatchAborted`.

### ConnSettings

`NewConnSettings()` returns the defaults. `NewConnSettingsFromMap` builds from
a map and **reports** unknown keys and wrong types instead of ignoring them.
`Clone()` deep-copies, including `CacheHeaders` and `Protocols`.

```go
settings, err := requester.NewConnSettingsFromMap(map[string]any{
    "Timeout":      "45s",           // time.Duration or a ParseDuration string
    "MaxIdleConns": 42,              // any integer type, or float64 from JSON
    "UseAsync":     true,
    "LogLevel":     "debug",
})
```

**Pooling**

| Setting | Default | Notes |
| --- | --- | --- |
| `UseKeepAlive` | `false` | **Off means no pooling at all.** Turn it on. |
| `MaxIdleConns` | `15` | Global idle ceiling across all hosts. |
| `MaxIdleConnsPerHost` | `15` | Idle connections kept per host. The stdlib default of 2 is the classic bottleneck. |
| `MaxConnsPerHost` | `30` | Total cap per host; excess requests block until one frees up. |

**Timeouts and retries**

| Setting | Default | Notes |
| --- | --- | --- |
| `Timeout` | `30s` | Overall per-request budget, redirects, retries and the body read included. Applied as a deadline on the request context, so expiry surfaces as `context.DeadlineExceeded`. |
| `HandShakeTimeout` | `10s` | TLS handshake. Doubles as the response-header timeout under `UseFailFast`. |
| `MaxRetries` | `0` | Transport failures only, never status codes. |
| `RetryDelay` | `200ms` | Fixed pause between attempts. |
| `UseFailFast` | `false` | Adds a response-header timeout, cancels batch siblings, and **disables retries**. |

**Requests and responses**

| Setting | Default | Notes |
| --- | --- | --- |
| `UseAsync` | `false` | Concurrent batches. |
| `UseMetrics` | `false` | Enables `GetMetrics()`. Compression is **not** a setting — see `GetCompressionHandler()`. |
| `MaxResponseBodySize` | `10 MiB` | Buffering cap; negative means unlimited. |
| `UseCachedHeaders` + `CacheHeaders` | `false` / nil | Headers added to every request unless already set. |
| `UseApiKey` + `ApiKey` | `false` / zero | An `ApiKey{Header, Value}` pair sent on every request. |
| `AllowRedirects` | `false` | Off means a 3xx is returned as-is. |
| `MaxRedirects` | `5` | Only consulted when redirects are allowed. |

**Connection and transport**

| Setting | Default | Notes |
| --- | --- | --- |
| `SkipPeerVerification` | `false` | Opt **out** of TLS verification. The zero value is the safe one. |
| `UseHTTP2` | `false` | Forces HTTP/2 on top of whatever `Protocols` says. |
| `HttpVersion` | `"1.1"` | Used when `Protocols` is empty. |
| `Protocols` | nil | Allowed protocols: `"http/1.1"`, `"h2"`, `"h2c"`, plus aliases such as `"1.1"`, `"2"` and `"http2"`. Overrides `HttpVersion`. |
| `UseProxy` + `ProxyURL` | `false` / `""` | An empty URL falls back to `HTTP_PROXY`/`HTTPS_PROXY`. |
| `UseCookies`, `CacheCookies`, `CookieJar` | `false` | Supply a jar, or let `CacheCookies` create one. |
| `UseLogging` + `LogLevel` | `false` / `INFO` | Only used when you pass a nil logger. |

Settings are read when the `Connection` is built. Retuning a `ConnSettings`
afterwards does nothing — that is why `GetCopySettings()` hands back a copy.
Most settings also have a dedicated option, which is the shorter route.

---

### ConnMetrics

Enabled by `ConnSettings.UseMetrics`. `GetMetrics()` returns
a snapshot with its own mutex, safe to read field by field.

| Field | Meaning |
| --- | --- |
| `CreatedAt`, `LastUsedAt` | Lifecycle timestamps. |
| `TotalRequests` | Finished requests, successes and failures. |
| `SuccessfulRequests`, `FailedRequests` | Split by whether a response arrived. |
| `TotalRetries` | Replayed attempts. |
| `TotalRedirects` | Redirects followed. |
| `TotalTimeouts` | Failures that were deadline or timeout errors. |
| `TotalConnectionErrors` | All other transport failures. |
| `AliveConnections` | Connections currently checked out. |
| `IdleConnections` | Connections returned to the pool. |
| `ClosedConnections` | Connections that could not be pooled. |

The connection counters come from `net/http/httptrace` and are **approximate**:
the transport reports a connection idle only once its body is closed, and
HTTP/2 multiplexes many requests onto a single connection. `Reset()` zeroes
everything.

### Compression

Reached with `conn.GetCompressionHandler()`. The handler owns the connection's
compression policy — there is no `UseCompression` setting — and also provides
standalone gzip/zlib helpers.

```go
comp := conn.GetCompressionHandler()

packed, err := comp.Compress(data, requester.GZIP)   // GZIP, ZLIB or NONE
plain, err := comp.Decompress(packed, requester.GZIP)

packed, err = comp.CompressFromResponse(resp, requester.GZIP)
plain, err = comp.DecompressFromResponse(resp)       // uses Content-Encoding
```

`NewCompressionHandler()` builds one detached from any connection when you only
want the helpers.

**Modes** decide who decodes the body on the wire. `net/http` only decompresses
a response when it added `Accept-Encoding` itself, so the header the handler
injects is what settles it:

| Mode | `Accept-Encoding` sent | Who decodes |
| --- | --- | --- |
| `CompressionTransport` (default) | added by `net/http` | `net/http`, transparently |
| `CompressionManual` | `gzip` | you, via `DecompressFromResponse` |
| `CompressionOff` | `identity` | nobody — the server sends plain bytes |

```go
comp.SetMode(requester.CompressionManual)
resp, _ := conn.GetRestHandler().Get(ctx, url, nil)
defer resp.Close()

body, err := comp.DecompressFromResponse(resp) // works: still encoded
```

`SetMode` is safe to call at any time; requests already in flight keep the
previous mode, and an explicit `Accept-Encoding` on a request always wins over
the mode.

> **`DecompressFromResponse` needs `CompressionManual`.** Under the default
> transport mode `net/http` has already decoded the body and stripped
> `Content-Encoding`, so there is nothing left to decode and the call returns
> `ErrNoContentEncoding`.

`Stats()` returns a `CompressionStats` snapshot — `Compressions`,
`Decompressions`, `CompressedBytes`, `DecompressedBytes` — and `ResetStats()`
zeroes them. `NONE` passes data through untouched and is not counted.

### Middleware

A `Middleware` wraps one request. Whatever runs before the call to `next`
happens on the way out; whatever runs after it happens on the way back. Both
halves live in one function, which is the whole point of the shape:

```go
type RoundTripFunc func(*http.Request) (*http.Response, error)
type Middleware func(req *http.Request, next RoundTripFunc) (*http.Response, error)
```

```go
func timing(log *slog.Logger) requester.Middleware {
    return func(req *http.Request, next requester.RoundTripFunc) (*http.Response, error) {
        start := time.Now()
        resp, err := next(req)
        log.Info("request",
            "method", req.Method,
            "url", req.URL.Redacted(),
            "took", time.Since(start),
            "err", err)
        return resp, err
    }
}

conn, err := requester.NewConnection(1, requester.WithMiddleware(timing(log)))
```

Register at construction with `WithMiddleware`, or at any point afterwards
through `conn.GetMiddleware()`. The chain is resolved per request, so a change
takes effect on the next one and requests already running keep the chain they
started with.

| Method | Description |
| --- | --- |
| `Add(mw) (MiddlewareID, error)` | Appends. Errors on nil. |
| `AddNamed(name, mw) (MiddlewareID, error)` | Same, with a label for `Names()`. |
| `Remove(id) error` | `ErrMiddlewareNotFound` if it is not registered. |
| `Clear()` | Removes everything. |
| `Len() int` / `Names() []string` | In run order. |

Ids are never reused, so removing a stale one is reported rather than silently
dropping whatever now sits at that position. Every method is safe on a nil
stack and safe to call concurrently, including from inside a request.

#### Where it sits

```
client -> mw[0] -> mw[1] -> ... -> headers, API key, retries -> transport
```

The middleware added first is outermost. Two consequences worth knowing:

- **Retries are downstream**, so a middleware runs **once per logical request**
  no matter how many attempts it takes, and the response it sees is the final
  one. If you want per-attempt behaviour, this is not the layer for it.
- **Cached headers and the API key are applied downstream too**, so a middleware
  does not see them. That keeps the key out of anything a logging middleware
  records; if a middleware needs to sign the final request, set the header from
  the middleware instead of using `WithApiKey`.

#### Rules

The request handed in is already a clone, so setting headers on it directly is
safe and does not disturb the caller's request. Beyond that, the two rules
inherited from `http.RoundTripper`:

- **Do not read or close the response body.** It belongs to the caller, and a
  middleware that drains it hands back an empty one.
- **Returning without calling `next` short-circuits the request.** That is a
  legitimate way to build a cache or a circuit breaker, but the response must be
  non-nil with a non-nil `Body`.

```go
// A circuit breaker: refuse to dial while the breaker is open.
stack.AddNamed("breaker", func(req *http.Request, next requester.RoundTripFunc) (*http.Response, error) {
    if breaker.Open() {
        if req.Body != nil {
            req.Body.Close() // a refused request still owns its body
        }
        return nil, errors.New("circuit open")
    }
    resp, err := next(req)
    breaker.Record(err)
    return resp, err
})
```

#### Built-in recipes

Ready-made middleware for the common cases. Each one is a constructor that
returns a `Middleware`, so they register like any other:

```go
conn, err := requester.NewConnection(1, requester.WithMiddleware(
    requester.MiddlewareLogger(log),
    requester.MiddlewareRequireHTTPS(),
    requester.MiddlewareAllowedHosts("api.example.com"),
    requester.MiddlewareBearerToken(tokens.Current),
    requester.MiddlewareRequestID(""),
))
```

**Guards** refuse a request before it is sent:

| Recipe | Refuses the request when | Error |
| --- | --- | --- |
| `MiddlewareSecureToken(header, token)` | `header` is absent, or its first value (trimmed) is not `token`. | `ErrAuthorizationHeaderMissing`, `ErrAuthorizationFailed` |
| `MiddlewareSecureHeaders(names)` | Any named header is absent or blank. | `ErrMissingHeader` |
| `MiddlewareSecureHeadersAndValues(map)` | A header is absent or lacks one of its expected values. Names match case-insensitively, values exactly. | `ErrMissingHeader` |
| `MiddlewareSecureQueryParams(names)` | Any named query parameter is absent or blank. | `ErrMissingQueryParam` |
| `MiddlewareSecureQueryParamsAndValues(map)` | A query parameter is absent or lacks one of its expected values. | `ErrMissingQueryParam` |
| `MiddlewareAllowedHosts(hosts...)` | The URL's host is not in the list. Compared case-insensitively, port ignored. | `ErrHostNotAllowed` |
| `MiddlewareRequireHTTPS()` | The scheme is not `https`. | `ErrInsecureScheme` |

**Decorators** change the request on its way out:

| Recipe | Effect |
| --- | --- |
| `MiddlewareSetHeaders(map)` | Sets every header in the map, overwriting what is there. |
| `MiddlewareBearerToken(source)` | Sets `Authorization: Bearer <token>`, calling `source(ctx)` per request so an expiring token can be refreshed. A source error fails the request with `ErrAuthorizationFailed`; an empty token with `ErrAuthorizationHeaderMissing`. |
| `MiddlewareRequestID(header)` | Sets a random 32-character hex id unless the request already has one. `""` means `X-Request-Id`. |
| `MiddlewareTimeout(d)` | Bounds the whole logical request — every retry and the body read — to `d`. Expiry surfaces as `context.DeadlineExceeded`. |

**Observers** watch without changing anything:

| Recipe | Effect |
| --- | --- |
| `MiddlewareLogger(logger)` | One line per logical request: method, redacted URL, status and duration, or the error. Headers are never logged. A nil logger means `slog.Default()`. |

How they behave:

- **Every rejection wraps its sentinel**, so match with `errors.Is` rather than
  comparing messages. The message names the header or parameter that failed.
- **A rejected request's body is closed**, as the `http.RoundTripper` contract
  requires; `http.Client` does not do it for you.
- **Arguments are copied at construction.** Editing the map or slice you passed
  in afterwards does not change the rule.
- **Bad arguments fail loudly.** An empty expected value, an empty token, a nil
  token source or a non-positive timeout builds a middleware that rejects every
  request with `ErrInvalidRecipe`, rather than a guard that lets everything
  through.
- **`MiddlewareSecureToken` does not see `WithApiKey`.** The API key is applied
  downstream of the stack, so guarding the header it sets rejects every
  request. Use the guard for tokens the caller or an earlier middleware sets.
- **`MiddlewareTimeout` releases its deadline when the body is closed**, not
  when the middleware returns, so close the response as usual.

### Errors and enums

| Sentinel | Raised when |
| --- | --- |
| `ErrNilRequest` | A nil entry in a batch, or a nil request to `DoRequest`. |
| `ErrBatchAborted` | A request a fail-fast batch never attempted. |
| `ErrConnectionShutdown` | Any request attempted after `Shutdown()`. |
| `ErrNilMiddleware` | A nil middleware was registered. |
| `ErrMiddlewareNotFound` | `Remove` was given an id that is not registered. |
| `ErrInvalidRecipe` | A middleware recipe was built with unusable arguments. |
| `ErrAuthorizationHeaderMissing` | A recipe found no authorization header or token. |
| `ErrAuthorizationFailed` | A recipe's token did not match, or its token source failed. |
| `ErrMissingHeader` | A recipe's required header or header value is absent. |
| `ErrMissingQueryParam` | A recipe's required query parameter or value is absent. |
| `ErrHostNotAllowed` | `MiddlewareAllowedHosts` refused the request's host. |
| `ErrInsecureScheme` | `MiddlewareRequireHTTPS` refused a plain-HTTP request. |
| `ErrNilHttpRequest` / `ErrNilRoundTripFunc` | A recipe was called directly with a nil request or `next`. |
| `ErrNilResponse` | Reading the body of a nil response. |
| `ErrBodyConsumed` | Buffering a body already handed to `Stream()`. |
| `ErrBodyTooLarge` | The body exceeded `MaxResponseBodySize`. |
| `ErrUnsupportedCompression` | An algorithm or `Content-Encoding` the handler cannot decode. |
| `ErrNoContentEncoding` | `DecompressFromResponse` on an already-decoded body. |

All are usable with `errors.Is`. `*StatusError` works with `errors.As`.

```go
type ConnectionStatus int  // Disconnected, Updating, Connected, Idle, ShutDown
type LogLevel string       // INFO, DEBUG, WARNING, ERROR
type HTTPMethod string     // GET, POST, PUT, DELETE, PATCH, HEAD, OPTIONS
type ApiKey struct { Header, Value string } // the header a key is sent in
type CompressionType string // NONE, GZIP, ZLIB
type CompressionMode int     // CompressionTransport, CompressionManual, CompressionOff
```

---

## Recipes

### Authentication and default headers

Headers that are the same on every call belong on the connection, not on each
call site:

```go
conn, err := requester.NewConnection(1,
    requester.WithApiKey("X-API-Key", os.Getenv("API_KEY")),
    requester.WithHeaders(map[string]string{
        "Accept":     "application/json",
        "User-Agent": "my-service/1.0",
    }),
)
```

The API key is a header name and a value, so the scheme is yours to choose:

```go
requester.WithApiKey("X-API-Key", key)                  // X-API-Key: <key>
requester.WithApiKey("Authorization", "Bearer "+token)  // Authorization: Bearer <token>
```

Both the key and the cached headers are applied only when the request does not
already set that header, so a per-call `header` argument always wins.

The key is fixed at construction. For a token that expires, fetch it per
request with a middleware instead:

```go
conn, err := requester.NewConnection(1, requester.WithMiddleware(
    requester.MiddlewareBearerToken(func(ctx context.Context) (string, error) {
        return tokens.Refresh(ctx) // your token cache
    }),
))
```

`ApiKey` redacts its value when printed, so `%v` on a settings value or a key
cannot leak the secret into a log. It **is** included in `MarshalJSON`, though,
so treat that output as a secret.

### Timeouts and cancellation

`Timeout` is the overall budget for a request, including redirects and retries.
For a shorter deadline on one call, use the context:

```go
ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
defer cancel()

resp, err := rest.Get(ctx, url, nil)
```

Cancelling a context stops retries immediately rather than waiting out
`RetryDelay`.

To cancel everything on the connection at once — on SIGINT, or when a request
handler that fanned out gives up — use the kill switch instead of threading one
context through every call site:

```go
go func() {
    <-ctx.Done()
    conn.Shutdown() // aborts what is running, refuses what comes next
}()
```

Note that a connection is **not** a place to park a context. There is no
"default context" setting, and there should not be: a context carries a deadline
that is already counting down, so one stored at construction would expire once
and take the connection with it. `Timeout` is the per-request budget, and it is
applied fresh every time.

### Retries

Retries cover **transport failures only** — a 500 is a successful round trip
and is never retried. A request with a body is only replayed if the body is
rewindable, which it is for `bytes.Reader`, `bytes.Buffer` and `strings.Reader`
(so `DoManualBatchRequests` buffers a shared body for exactly this reason).

```go
conn, err := requester.NewConnection(1,
    requester.WithRetries(3, 500*time.Millisecond),
)
```

Do not combine this with `WithFailFast()`. Fail-fast and retrying are
contradictory, and fail-fast wins: the transport zeroes `MaxRetries` whenever
`UseFailFast` is set. Option order makes no difference — putting `WithRetries`
last does not rescue it.

### Large downloads

Do not buffer a file into memory:

```go
resp, err := rest.Get(ctx, fileURL, nil)
if err != nil {
    return err
}
defer resp.Close()

file, err := os.Create("download.bin")
if err != nil {
    return err
}
defer file.Close()

written, err := resp.Save(file) // ignores MaxResponseBodySize
```

### Redirects

Redirects are **off** by default, so a 3xx arrives as a normal response:

```go
conn, err := requester.NewConnection(1, requester.WithRedirects(10))
```

Exceeding the cap returns an error from the request.

### Cookies

```go
conn, err := requester.NewConnection(1, requester.WithCookies())

// or share one session across connections:
jar, _ := cookiejar.New(nil)
conn, err = requester.NewConnection(2, requester.WithCookieJar(jar))
```

### Proxies

```go
conn, err := requester.NewConnection(1,
    requester.WithProxy("http://proxy.internal:8080"),
)

// or take it from HTTP_PROXY / HTTPS_PROXY / NO_PROXY:
conn, err = requester.NewConnection(2, requester.WithProxyFromEnvironment())
```

`WithProxy` validates the URL and fails construction if it has no scheme or
host. Setting `ProxyURL` through the settings instead only logs the problem and
carries on **without** a proxy, which is easy to miss.

### HTTP/2

```go
conn, err := requester.NewConnection(1, requester.WithHTTP2())

// or be explicit; an unknown name is rejected rather than ignored:
conn, err = requester.NewConnection(2, requester.WithProtocols("h2", "http/1.1"))
```

### Warming the pool

```go
conn, err := requester.NewConnection(1, requester.WithKeepAlive(8, 16))
if err != nil {
    return err
}
if err := conn.ConnectionWarmUp(ctx, "https://api.example.com", 8); err != nil {
    return err
}
```

The requests are issued concurrently on purpose: sequential ones would each
reuse the connection the previous one released and only ever warm up one.

---

## Tuning the pool

The transport already is the connection pool.

- **`MaxIdleConnsPerHost` is the one that bites.** If it is lower than your
  real concurrency, connections are opened, used once and closed instead of
  pooled. For a client that hammers a single host, set it equal to
  `MaxConnsPerHost`.
- **`MaxConnsPerHost` is backpressure.** Once reached, further requests block
  until a connection frees up, cancellable by context. Set too low, it looks
  like a hang.
- **Idle connections are dropped after 90 seconds.** If a server closes idle
  connections sooner, you will see sporadic `EOF` errors from racing its close.
- **HTTP/2 changes the arithmetic.** One connection multiplexes many streams,
  so per-host connection counts stop meaning much.

Enable metrics and watch `IdleConnections` versus `AliveConnections` to see
whether reuse is actually happening.

---

## Gotchas

- **`UseKeepAlive` defaults to `false`**, which disables pooling entirely and
  makes `ConnectionWarmUp` return an error. Turn it on for anything that makes
  more than one request.
- **An unread body still owns a connection.** `Close()` drains up to 64 KiB to
  reclaim it; past that the connection is dropped rather than drained. To skip
  a large body and keep the connection, call `Discard()`, which drains up to
  `MaxResponseBodySize`.
- **A batch larger than `MaxConnsPerHost` stalls.** A batch holds every
  response open until it returns, so once the per-host limit is reached the
  remaining requests wait for a connection that is never freed. The batch
  stalls for the full `Timeout`, which then fails either the waiting requests
  or the bodies being held. Keep batches at or below the limit, or split them.
- **A 4xx/5xx is not an error.** Check `ErrorForStatus()` or `IsSuccess()`.
- **`GetCopySettings()` returns a copy.** Configure the connection with options
  at construction, not by retuning settings afterwards.
- **`Status()` is informational.** Only three things move it: a successful
  `ConnectionWarmUp` sets `Connected`, `Close()` sets `Disconnected`, and
  `Shutdown()` sets `ShutDown` permanently. Ordinary requests do not touch it,
  and `Idle` and `Updating` are never set.

---

## Testing

```sh
go test ./...            # everything, including tests that hit the network
go test ./... -short     # skips the network-dependent tests
go test ./... -v         # per-test output
```

Every test lives in `tests/` as package `tests`, so the suite exercises only
the exported API — the same surface a consumer sees. Nothing reaches into
unexported fields.

| File | Covers |
| --- | --- |
| `helpers_test.go` | Shared fixtures: `newTestConn`, `serve`, `closeBodies`. |
| `connection_test.go` | Batching, retries, header injection, warm-up, redirects, settings, metrics. |
| `options_test.go` | Option validation, ordering, error collection, `WithSettings` rules, API keys. |
| `response_test.go` | Body helpers, size limits, streaming, status errors, nil-safety, TLS defaults. |
| `compression_test.go` | Gzip/zlib round trips and the `Content-Encoding` interaction. |
| `shutdown_test.go` | The kill switch: rejection, in-flight cancellation, in-flight bookkeeping. |
| `middleware_test.go` | Chain order, header rewriting, short-circuiting, live editing. |
| `middleware_recipes_test.go` | Built-in recipes: sentinel wrapping, case-insensitive headers, body closing, config copying, timeouts, logging. |
| `conn_test.go` | End-to-end calls against a live public API. |

All but `conn_test.go` use `httptest` servers and real TCP listeners, so they
need no network access. The live-API tests are skipped under `-short`.

### Benchmarks

`tests/bench` measures speed and memory against local `httptest` servers, so
the numbers are client overhead plus loopback, never the network. The latest
results and their analysis are in [docs/BENCHMARKS.md](docs/BENCHMARKS.md).

```sh
go test ./tests/bench -run '^$' -bench . -benchmem                     # everything
go test ./tests/bench -short -run '^$' -bench . -benchmem -count 10 > new.txt
benchstat old.txt new.txt                                               # compare runs
```

| File | Measures |
| --- | --- |
| `requests_bench_test.go` | Construction, GET/POST by body size, parallel GETs, keep-alive on/off, the cost of each optional feature, and a bare `net/http` baseline to compare against. |
| `batch_bench_test.go` | Sync vs async batches, and the shared-body batch. |
| `middleware_bench_test.go` | Cost per middleware as the stack deepens, the built-in recipes, live stack edits. |
| `response_bench_test.go` | `Bytes` vs `Stream` vs `Save` vs `Discard`, and `JSON` decoding. |
| `compression_bench_test.go` | Gzip and zlib throughput and ratio. |
| `memory_bench_test.go` | Heap and goroutines per pooled connection, heap growth under sustained load, `Shutdown` cost, and a goroutine-leak test that also runs with `go test ./...`. |

Custom metrics appear next to `ns/op`: `heap-B/conn`, `goroutines/conn`,
`heap-growth-KiB`, `ns/request`, `shutdown-ns` and `ratio`.

Two benchmarks dial a new TCP connection per iteration:
`BenchmarkKeepAlive/off` and `BenchmarkConnectionFootprint`. Each dial leaves
a socket in TIME_WAIT for minutes, and a few full runs in a row can use up the
default Windows range of about 16,000 ephemeral ports. Past that limit, every
benchmark after them fails to dial. `-short` skips those two, so use it for
`-count` runs.

---

## Known limitations

- Retry backoff is a fixed delay — no exponential backoff or jitter.
