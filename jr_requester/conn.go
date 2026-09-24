package jr_requester

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptrace"
	"sync"
	"sync/atomic"
	"time"
)

// ------------
// Errors
// ------------

var (
	// ErrNilRequest is reported for a nil entry in a batch.
	ErrNilRequest = errors.New("nil request")
	// ErrBatchAborted marks the requests a fail-fast batch never attempted, so
	// a skipped entry is never mistaken for a success with no response.
	ErrBatchAborted = errors.New("request skipped: batch aborted after an earlier failure")
	// ErrConnectionShutdown is returned by every request attempted after
	// Shutdown. Unlike Close, a shutdown connection never comes back.
	ErrConnectionShutdown = errors.New("connection has been shut down")
)

// ------------
// Structs
// ------------

type Connection struct {
	// Optional Id
	id uint64
	// The underlying HTTP client used for making requests.
	client *http.Client
	// The underlying HTTP transport used for managing connections.
	transport *http.Transport
	// The settings used to configure the connection.
	settings *ConnSettings
	// Setting accessors
	accessors *SettingsAccessor
	// Metrics for tracking connection and request performance. Nil when
	// metrics are disabled; every recording helper tolerates a nil receiver.
	metrics *ConnMetrics
	// A logger for logging connection events and errors.
	logger *slog.Logger
	// REST handler for making HTTP requests.
	rest *RestHandler
	// Compression handler, which owns this connection's compression policy.
	compression *CompressionHandler
	// Middleware runs in front of the retry layer and stays editable on a
	// live connection.
	middleware *MiddlewareStack
	// timeout is the per-request budget. It is applied to the request context
	// in DoRequest rather than as http.Client.Timeout: see DoRequest.
	timeout time.Duration
	// decorated is set when the transport is wrapped in a requestDecorator,
	// one of the layers that edits the request on its way down.
	decorated bool

	// mtx guards status, shutdown and inflight, which batch goroutines and
	// callers may touch at once.
	mtx sync.RWMutex
	// The current status of the connection.
	status ConnectionStatus
	// shutdown is set once by Shutdown and never cleared.
	shutdown bool
	// inflight holds the cancel of every request that has not finished with
	// its body yet, keyed by nextInflight. Shutdown fires all of them.
	inflight map[uint64]context.CancelFunc
	// nextInflight hands out the inflight keys.
	nextInflight uint64
}

// releaseOnClose runs release once the body is closed. A response body is read
// under its request's context, so the per-request cancel has to outlive
// DoRequest and can only be dropped when the caller is done with the body.
type releaseOnClose struct {
	io.ReadCloser
	once    sync.Once
	release func()
}

func (b *releaseOnClose) Close() error {
	err := b.ReadCloser.Close()
	b.once.Do(b.release)
	return err
}

// ownedRequestKey marks the context of a request DoRequest built: its own
// clone, and the per-hop requests http.Client derives from it for redirects.
// Nobody else holds those, so the layers below may edit them in place rather
// than each taking another clone. A request without the mark, such as one sent
// through GetClient directly, still gets cloned.
type ownedRequestKey struct{}

// ownsRequest reports whether req carries the ownedRequestKey mark.
func ownsRequest(req *http.Request) bool {
	owned, _ := req.Context().Value(ownedRequestKey{}).(bool)
	return owned
}

// RestHandler exposes the convenience verb methods for a Connection.
type RestHandler struct {
	conn *Connection
}

// ------------
// Constructors
// ------------

// NewConnection builds a connection from the options given, starting from the
// defaults:
//
//	conn, err := NewConnection(1,
//		WithKeepAlive(16, 32),
//		WithRetries(3, 500*time.Millisecond),
//		WithApiKey(os.Getenv("API_KEY")),
//	)
//
// With no options it returns a working connection on the defaults. Options are
// applied in order and the later one wins; every option that rejects its
// argument is reported, not just the first.
func NewConnection(id uint64, opts ...ConnOption) (*Connection, error) {
	cfg := &connConfig{
		settings:        NewConnSettings(),
		compressionMode: CompressionTransport,
		middleware:      NewMiddlewareStack(),
	}

	var errs []error
	for _, opt := range opts {
		if opt == nil {
			continue
		}
		if err := opt(cfg); err != nil {
			errs = append(errs, err)
		}
		cfg.applied++
	}
	if err := errors.Join(errs...); err != nil {
		return nil, err
	}

	// Take a copy: the transport is built once, here, so a caller mutating
	// the settings they passed in afterwards must not change this connection.
	settings := cfg.settings.Clone()

	logger := cfg.logger
	if logger == nil {
		// Build one from UseLogging/LogLevel when none was supplied.
		logger = newDefaultLogger(settings)
	}

	var metrics *ConnMetrics
	if settings.useMetrics {
		metrics = NewConnMetrics()
	}

	transport := settings.newTransport(logger)
	roundTripper := settings.newRoundTripper(transport, metrics)
	_, decorated := roundTripper.(*requestDecorator)
	// The stack is resolved per request, so wiring it in here does not freeze
	// the middleware the caller registered at construction.
	middleware := cfg.middleware
	conn := &Connection{
		id:        id,
		status:    Disconnected,
		transport: transport,
		client: &http.Client{
			// No Timeout: DoRequest puts it on the request context instead.
			Jar:           settings.resolveCookieJar(logger),
			CheckRedirect: settings.checkRedirect(metrics),
			Transport:     middleware.wrap(roundTripper),
		},
		settings:   settings,
		metrics:    metrics,
		logger:     logger,
		inflight:   make(map[uint64]context.CancelFunc),
		middleware: middleware,
		timeout:    settings.effectiveTimeout(),
		decorated:  decorated,
	}
	conn.rest = newRestHandler(conn)
	conn.compression = NewCompressionHandler()
	// The mode was validated by the option, so this cannot fail.
	_ = conn.compression.SetMode(cfg.compressionMode)
	return conn, nil
}

func newRestHandler(conn *Connection) *RestHandler {
	return &RestHandler{conn: conn}
}

// ------------
// Methods
// ------------

func (c *Connection) String() string {
	if c.metrics == nil {
		return fmt.Sprintf("Connection{id: %d, status: %v, metrics: disabled}", c.id, c.Status())
	}
	m := c.metrics.Clone()
	return fmt.Sprintf("Connection{id: %d, status: %v, requests: %d, alive: %d, idle: %d, closed: %d}",
		c.id, c.Status(), m.TotalRequests, m.AliveConnections, m.IdleConnections, m.ClosedConnections)
}

// Getters

// CopySettings returns a copy of the settings. Mutating the original after the
// connection is built would not reconfigure the live transport, so handing out
// the pointer would only invite drift between the two.
func (c *Connection) GetCopySettings() *ConnSettings {
	return c.settings.Clone()
}

// GetMetrics returns a snapshot of the metrics, or nil if they are disabled.
func (c *Connection) GetMetrics() *ConnMetrics {
	return c.metrics.Clone()
}

func (c *Connection) GetTransport() *http.Transport {
	return c.transport
}

// GetClient returns the underlying client, as an escape hatch. It has no
// Timeout of its own, since DoRequest applies the connection's through the
// request context, and requests sent through it directly bypass the in-flight
// tracking Shutdown relies on. Prefer DoRequest.
func (c *Connection) GetClient() *http.Client {
	return c.client
}

func (c *Connection) GetRestHandler() *RestHandler {
	return c.rest
}

// GetCompressionHandler returns the handler that owns this connection's
// compression policy and provides the gzip/zlib helpers.
func (c *Connection) GetCompressionHandler() *CompressionHandler {
	return c.compression
}

// GetMiddleware returns the connection's middleware stack. It is live: adding
// or removing a middleware takes effect on the next request, and requests
// already running keep the chain they started with.
func (c *Connection) GetMiddleware() *MiddlewareStack {
	return c.middleware
}

// Status reports the current connection status.
func (c *Connection) Status() ConnectionStatus {
	c.mtx.RLock()
	defer c.mtx.RUnlock()
	return c.status
}

func (c *Connection) setStatus(status ConnectionStatus) {
	c.mtx.Lock()
	defer c.mtx.Unlock()
	// ShutDown is terminal, so a later Close must not report the connection as
	// merely disconnected and reusable.
	if c.shutdown {
		return
	}
	c.status = status
}

// ------------
// Requests
// ------------

// DoRequest executes a single HTTP request and returns the response. The caller
// owns the Response and must drain and close its body, or the connection will
// not go back into the pool.
func (c *Connection) DoRequest(ctx context.Context, req *http.Request) (*http.Response, error) {
	if req == nil {
		return nil, ErrNilRequest
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if c.metrics != nil {
		ctx = httptrace.WithClientTrace(ctx, c.metrics.clientTrace())
	}

	// The timeout lives on the context rather than on http.Client.Timeout.
	// net/http only enforces Client.Timeout cheaply for a transport it
	// recognises; this one is wrapped, so it used to cost a timer and a
	// goroutine per request. The context stays live until the body is closed,
	// so the body read is still inside the budget.
	ctx, cancel := context.WithTimeout(ctx, c.timeout)
	// The layers below may edit this request in place instead of cloning it
	// again. Only worth the extra context when one of them is there.
	if c.decorated || c.middleware.Len() > 0 {
		ctx = context.WithValue(ctx, ownedRequestKey{}, true)
	}
	id, ok := c.registerInflight(cancel)
	if !ok {
		cancel()
		return nil, ErrConnectionShutdown
	}
	release := func() {
		c.releaseInflight(id)
		cancel()
	}

	outgoing := req.Clone(ctx)
	// The compression handler, not a setting, decides what we ask for.
	c.compression.applyTo(outgoing)

	resp, err := c.client.Do(outgoing)
	c.metrics.recordRequest(err)
	if err != nil || resp == nil || resp.Body == nil {
		release()
		return resp, err
	}
	resp.Body = &releaseOnClose{ReadCloser: resp.Body, release: release}
	return resp, nil
}

// registerInflight records a request's cancel and reports false once the
// connection has been shut down.
func (c *Connection) registerInflight(cancel context.CancelFunc) (uint64, bool) {
	c.mtx.Lock()
	defer c.mtx.Unlock()
	if c.shutdown {
		return 0, false
	}
	c.nextInflight++
	if c.inflight == nil {
		c.inflight = make(map[uint64]context.CancelFunc)
	}
	c.inflight[c.nextInflight] = cancel
	return c.nextInflight, true
}

// releaseInflight forgets a request that has finished with its body.
func (c *Connection) releaseInflight(id uint64) {
	c.mtx.Lock()
	defer c.mtx.Unlock()
	delete(c.inflight, id)
}

// DoBatchRequests runs every request and returns the results in input order,
// concurrently when UseAsync is set and sequentially otherwise. The caller owns
// every non-nil Response and must drain and close its body, or the connection
// will not go back into the pool.
func (c *Connection) DoBatchRequests(ctx context.Context, requests []*http.Request) []BatchResult {
	if c.settings.useAsync {
		return c.batchAsync(ctx, requests)
	}
	return c.batchSync(ctx, requests)
}

// batchAsync runs the requests concurrently, bounded by MaxConnsPerHost.
func (c *Connection) batchAsync(ctx context.Context, requests []*http.Request) []BatchResult {
	if ctx == nil {
		ctx = context.Background()
	}
	results := make([]BatchResult, len(requests))
	if len(requests) == 0 {
		return results
	}

	// Fail fast cancels the siblings the moment one request errors. The bodies
	// are read under this context too, so it is released once the batch and
	// every body it handed out are done, not when the batch returns: that
	// would cancel every body the caller has not read yet.
	ctx, cancel := context.WithCancel(ctx)
	var open atomic.Int64
	open.Add(1) // held by the batch itself until every request has run
	release := func() {
		if open.Add(-1) == 0 {
			cancel()
		}
	}
	defer release()

	// Bound the fan-out so a huge batch does not park a goroutine per request
	// on the transport's per-host connection limit.
	limit := int(c.settings.maxConnsPerHost)
	if limit <= 0 || limit > len(requests) {
		limit = len(requests)
	}
	slots := make(chan struct{}, limit)

	var wg sync.WaitGroup
	for i, req := range requests {
		// wg.Go pairs the Add with the Done, so a return path cannot skip it.
		wg.Go(func() {
			slots <- struct{}{}
			defer func() { <-slots }()

			results[i].Request = req
			if req == nil {
				results[i].Err = ErrNilRequest
				return
			}
			if err := ctx.Err(); err != nil {
				results[i].Err = err
				return
			}

			resp, err := c.DoRequest(ctx, req)
			if err == nil && resp != nil && resp.Body != nil {
				open.Add(1)
				resp.Body = &releaseOnClose{ReadCloser: resp.Body, release: release}
			}
			results[i].Response, results[i].Err = c.wrap(resp), err
			if err != nil {
				c.logger.Error("batch request failed", "url", req.URL.Redacted(), "error", err)
				if c.settings.useFailFast {
					cancel()
				}
			}
		})
	}

	wg.Wait()
	return results
}

// batchSync runs the requests one after another on the calling goroutine.
func (c *Connection) batchSync(ctx context.Context, requests []*http.Request) []BatchResult {
	if ctx == nil {
		ctx = context.Background()
	}
	results := make([]BatchResult, len(requests))

	for i, req := range requests {
		results[i].Request = req
		if req == nil {
			results[i].Err = ErrNilRequest
			continue
		}

		resp, err := c.DoRequest(ctx, req)
		results[i].Response, results[i].Err = c.wrap(resp), err
		if err == nil {
			continue
		}

		c.logger.Error("batch request failed", "url", req.URL.Redacted(), "error", err)
		if c.settings.useFailFast {
			for j := i + 1; j < len(requests); j++ {
				results[j].Request = requests[j]
				results[j].Err = ErrBatchAborted
			}
			break
		}
	}
	return results
}

// DoManualRequest builds a request from its parts and executes it, returning
// the wrapped Response. The caller must close it.
func (c *Connection) DoManualRequest(ctx context.Context, method HTTPMethod, rawURL string, header map[string]string, body io.Reader) (*Response, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	req, err := buildRequest(ctx, method, rawURL, header, body)
	if err != nil {
		return nil, err
	}
	resp, err := c.DoRequest(ctx, req)
	if err != nil {
		return nil, err
	}
	return c.wrap(resp), nil
}

// DoManualBatchRequests sends the same body to every URL.
func (c *Connection) DoManualBatchRequests(ctx context.Context, method HTTPMethod, urls []string, header map[string]string, body io.Reader) []BatchResult {
	bodies := make([]io.Reader, len(urls))

	if body != nil {
		buffered, err := io.ReadAll(body)
		if err != nil {
			results := make([]BatchResult, len(urls))
			for i := range results {
				results[i].Err = fmt.Errorf("failed to buffer the shared request body: %w", err)
			}
			return results
		}
		for i := range bodies {
			bodies[i] = bytes.NewReader(buffered)
		}
	}

	return c.DoManualBatchRequestsWithBodies(ctx, method, urls, header, bodies)
}

// DoManualBatchRequestsWithBodies pairs each URL with the body at the same
// index. A shorter bodies slice leaves the trailing requests without a body.
func (c *Connection) DoManualBatchRequestsWithBodies(ctx context.Context, method HTTPMethod, urls []string, header map[string]string, bodies []io.Reader) []BatchResult {
	if ctx == nil {
		ctx = context.Background()
	}

	requests := make([]*http.Request, len(urls))
	buildErrs := make([]error, len(urls))
	for i, rawURL := range urls {
		var body io.Reader
		if i < len(bodies) {
			body = bodies[i]
		}

		req, err := buildRequest(ctx, method, rawURL, header, body)
		if err != nil {
			buildErrs[i] = fmt.Errorf("failed to build request for %q: %w", rawURL, err)
			continue
		}
		requests[i] = req
	}

	results := c.DoBatchRequests(ctx, requests)

	// Replace the generic nil-request error with the real cause.
	for i, err := range buildErrs {
		if err != nil {
			results[i].Err = err
		}
	}
	return results
}

// buildRequest assembles a request from its parts.
func buildRequest(ctx context.Context, method HTTPMethod, rawURL string, header map[string]string, body io.Reader) (*http.Request, error) {
	req, err := http.NewRequestWithContext(ctx, string(method), rawURL, body)
	if err != nil {
		return nil, err
	}
	for key, value := range header {
		req.Header.Set(key, value)
	}
	return req, nil
}

// ------------
// Connection Management
// ------------

// ConnectionWarmUp fills the pool with connNumber connections to urlHost.
//
// Warmed connections only survive if keep-alive is on and MaxIdleConnsPerHost
// is at least connNumber; otherwise the surplus is closed straight away.
func (c *Connection) ConnectionWarmUp(ctx context.Context, urlHost string, connNumber int) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if connNumber <= 0 {
		return fmt.Errorf("connNumber must be positive, got %d", connNumber)
	}
	if !c.settings.useKeepAlive {
		return errors.New("cannot warm up connections while keep-alive is disabled")
	}
	if int(c.settings.maxIdleConnsPerHost) < connNumber {
		c.logger.Warn("warming up more connections than the pool will keep",
			"connNumber", connNumber, "maxIdleConnsPerHost", c.settings.maxIdleConnsPerHost)
	}

	errs := make([]error, connNumber)
	var wg sync.WaitGroup
	for i := range connNumber {
		wg.Go(func() {
			req, err := http.NewRequestWithContext(ctx, http.MethodHead, urlHost, nil)
			if err != nil {
				errs[i] = fmt.Errorf("failed to build warm-up request: %w", err)
				return
			}

			resp, err := c.DoRequest(ctx, req)
			if err != nil {
				errs[i] = fmt.Errorf("failed to connect: %w", err)
				return
			}
			resp.Body.Close()

			if resp.StatusCode < 200 || resp.StatusCode >= 400 {
				errs[i] = fmt.Errorf("failed to connect, status code: %d", resp.StatusCode)
			}
		})
	}
	wg.Wait()

	if err := errors.Join(errs...); err != nil {
		return err
	}

	c.setStatus(Connected)
	return nil
}

// Close drops every pooled connection. In-flight requests are unaffected; the
// connection stays usable afterwards and will simply dial again. To stop the
// work already running, use Shutdown.
func (c *Connection) Close() {
	if c.transport != nil {
		c.transport.CloseIdleConnections()
	}
	c.setStatus(Disconnected)
}

// Shutdown is the kill switch: it cancels every request still in flight, drops
// the pooled connections and puts the connection permanently out of service.
// Later requests fail with ErrConnectionShutdown rather than dialling again.
//
// It returns once the cancellations have been delivered, not once the callers
// have noticed; each in-flight request fails with a context.Canceled error on
// its own goroutine. Calling it more than once is safe, and unlike Close it
// cannot be undone, so keep it for teardown:
//
//	conn, err := NewConnection(1)
//	...
//	defer conn.Shutdown()
func (c *Connection) Shutdown() {
	c.mtx.Lock()
	if c.shutdown {
		c.mtx.Unlock()
		return
	}
	c.shutdown = true
	c.status = ShutDown
	// Take the cancels out under the lock but call them outside it: a cancel
	// wakes the request goroutine, which calls straight back in to release its
	// entry and would deadlock against a lock still held here.
	cancels := make([]context.CancelFunc, 0, len(c.inflight))
	for id, cancel := range c.inflight {
		cancels = append(cancels, cancel)
		delete(c.inflight, id)
	}
	c.mtx.Unlock()

	for _, cancel := range cancels {
		cancel()
	}
	if c.transport != nil {
		c.transport.CloseIdleConnections()
	}
	c.logger.Info("connection shut down", "id", c.id, "cancelled", len(cancels))
}

// InFlight reports how many requests are still holding a connection: those
// whose response body has not been closed yet. It is the count Shutdown would
// cancel right now.
func (c *Connection) InFlight() int {
	c.mtx.RLock()
	defer c.mtx.RUnlock()
	return len(c.inflight)
}

// IsShutdown reports whether Shutdown has been called.
func (c *Connection) IsShutdown() bool {
	c.mtx.RLock()
	defer c.mtx.RUnlock()
	return c.shutdown
}

// ------------
// HTTP Single Rest
// ------------

// Get executes a GET request to the specified URL with the provided headers.
func (h *RestHandler) Get(ctx context.Context, rawURL string, header map[string]string) (*Response, error) {
	return h.conn.DoManualRequest(ctx, GET, rawURL, header, nil)
}

// Post executes a POST request to the specified URL with the provided headers and body.
func (h *RestHandler) Post(ctx context.Context, rawURL string, header map[string]string, body io.Reader) (*Response, error) {
	return h.conn.DoManualRequest(ctx, POST, rawURL, header, body)
}

// Put executes a PUT request to the specified URL with the provided headers and body.
func (h *RestHandler) Put(ctx context.Context, rawURL string, header map[string]string, body io.Reader) (*Response, error) {
	return h.conn.DoManualRequest(ctx, PUT, rawURL, header, body)
}

// Delete executes a DELETE request to the specified URL with the provided headers.
func (h *RestHandler) Delete(ctx context.Context, rawURL string, header map[string]string) (*Response, error) {
	return h.conn.DoManualRequest(ctx, DELETE, rawURL, header, nil)
}

// Patch executes a PATCH request to the specified URL with the provided headers and body.
func (h *RestHandler) Patch(ctx context.Context, rawURL string, header map[string]string, body io.Reader) (*Response, error) {
	return h.conn.DoManualRequest(ctx, PATCH, rawURL, header, body)
}

// Head executes a HEAD request to the specified URL with the provided headers.
func (h *RestHandler) Head(ctx context.Context, rawURL string, header map[string]string) (*Response, error) {
	return h.conn.DoManualRequest(ctx, HEAD, rawURL, header, nil)
}

// Options executes an OPTIONS request to the specified URL with the provided headers.
func (h *RestHandler) Options(ctx context.Context, rawURL string, header map[string]string) (*Response, error) {
	return h.conn.DoManualRequest(ctx, OPTIONS, rawURL, header, nil)
}

// ------------
// HTTP Batch Requests
// ------------

// BatchGet executes a GET request against each URL with the provided headers.
func (h *RestHandler) BatchGet(ctx context.Context, urls []string, header map[string]string) []BatchResult {
	return h.conn.DoManualBatchRequests(ctx, GET, urls, header, nil)
}

// BatchPost executes a POST request against each URL, pairing urls[i] with bodies[i].
func (h *RestHandler) BatchPost(ctx context.Context, urls []string, header map[string]string, bodies []io.Reader) []BatchResult {
	return h.conn.DoManualBatchRequestsWithBodies(ctx, POST, urls, header, bodies)
}

// BatchPut executes a PUT request against each URL, pairing urls[i] with bodies[i].
func (h *RestHandler) BatchPut(ctx context.Context, urls []string, header map[string]string, bodies []io.Reader) []BatchResult {
	return h.conn.DoManualBatchRequestsWithBodies(ctx, PUT, urls, header, bodies)
}

// BatchDelete executes a DELETE request against each URL with the provided headers.
func (h *RestHandler) BatchDelete(ctx context.Context, urls []string, header map[string]string) []BatchResult {
	return h.conn.DoManualBatchRequests(ctx, DELETE, urls, header, nil)
}

// BatchPatch executes a PATCH request against each URL, pairing urls[i] with bodies[i].
func (h *RestHandler) BatchPatch(ctx context.Context, urls []string, header map[string]string, bodies []io.Reader) []BatchResult {
	return h.conn.DoManualBatchRequestsWithBodies(ctx, PATCH, urls, header, bodies)
}

// BatchHead executes a HEAD request against each URL with the provided headers.
func (h *RestHandler) BatchHead(ctx context.Context, urls []string, header map[string]string) []BatchResult {
	return h.conn.DoManualBatchRequests(ctx, HEAD, urls, header, nil)
}

// BatchOptions executes an OPTIONS request against each URL with the provided headers.
func (h *RestHandler) BatchOptions(ctx context.Context, urls []string, header map[string]string) []BatchResult {
	return h.conn.DoManualBatchRequests(ctx, OPTIONS, urls, header, nil)
}
