package jr_requester

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime"
	"net/http"
	"strings"
	"sync"
)

// ------------
// Errors
// ------------

var (
	// ErrNilResponse is returned by the accessors of a nil or empty Response.
	ErrNilResponse = errors.New("nil response")
	// ErrBodyConsumed is returned when the body was already handed to Stream
	// and therefore cannot be buffered.
	ErrBodyConsumed = errors.New("response body already consumed by Stream")
	// ErrBodyTooLarge is returned when the body exceeds MaxResponseBodySize.
	ErrBodyTooLarge = errors.New("response body exceeds the configured limit")
)

// maxDrainOnClose bounds how much of an unread body Close will drain to make
// the connection reusable. Past this point draining costs more than dialling
// again, so the connection is dropped instead.
const maxDrainOnClose = 64 << 10

// snippetLength is how much of a failing body StatusError quotes.
const snippetLength = 512

// ------------
// Structs
// ------------

// Response wraps an *http.Response with helpers that read the body safely.
//
// The body is read at most once and then cached, so Bytes, Text and JSON can
// be called in any order and repeatedly. Every one of them closes the
// underlying body, which is what returns the connection to the pool; calling
// Close as well is harmless and is the safe habit for paths that might not
// read the body at all.
type Response struct {
	raw *http.Response
	// limit is the maximum body size to buffer. Negative means unlimited.
	limit int64

	mtx      sync.Mutex
	body     []byte
	bodyErr  error
	buffered bool
	streamed bool
	closed   bool
}

// StatusError reports a non-2xx status, quoting the start of the body because
// the reason an API rejected a call is almost always in there.
type StatusError struct {
	StatusCode int
	Status     string
	URL        string
	Snippet    string
}

func (e *StatusError) Error() string {
	status := e.Status
	if status == "" {
		status = fmt.Sprintf("%d", e.StatusCode)
	}
	if e.URL != "" {
		status = fmt.Sprintf("%s: %s", e.URL, status)
	}
	if e.Snippet == "" {
		return fmt.Sprintf("http status %s", status)
	}
	return fmt.Sprintf("http status %s: %s", status, e.Snippet)
}

// ------------
// Constructors
// ------------

// newResponse wraps a raw response. It returns nil when raw is nil so that a
// failed request yields a nil *Response rather than an empty shell.
func newResponse(raw *http.Response, limit int64) *Response {
	if raw == nil {
		return nil
	}
	return &Response{raw: raw, limit: limit}
}

// wrap builds a Response using this connection's body-size limit.
func (c *Connection) wrap(raw *http.Response) *Response {
	return newResponse(raw, c.settings.effectiveMaxResponseBodySize())
}

// ------------
// Metadata
// ------------

// String summarises the response without touching the body.
func (r *Response) String() string {
	if r == nil || r.raw == nil {
		return "Response{<nil>}"
	}
	return fmt.Sprintf("Response{status: %s, proto: %s, contentType: %q, contentLength: %d, url: %s}",
		r.Status(), r.Proto(), r.ContentType(), r.ContentLength(), r.URL())
}

// Raw exposes the underlying response for anything this wrapper does not
// cover. Reading Raw().Body directly bypasses the buffering and the limit.
func (r *Response) Raw() *http.Response {
	if r == nil {
		return nil
	}
	return r.raw
}

// StatusCode reports the HTTP status code, or 0 for a nil Response.
func (r *Response) StatusCode() int {
	if r == nil || r.raw == nil {
		return 0
	}
	return r.raw.StatusCode
}

// Status reports the HTTP status line, e.g. "404 Not Found".
func (r *Response) Status() string {
	if r == nil || r.raw == nil {
		return ""
	}
	return r.raw.Status
}

// Proto reports the protocol the response arrived on, e.g. "HTTP/2.0".
func (r *Response) Proto() string {
	if r == nil || r.raw == nil {
		return ""
	}
	return r.raw.Proto
}

// Header returns the response headers, never nil.
func (r *Response) Header() http.Header {
	if r == nil || r.raw == nil || r.raw.Header == nil {
		return http.Header{}
	}
	return r.raw.Header
}

// ContentLength reports the declared body length, or -1 when unknown.
func (r *Response) ContentLength() int64 {
	if r == nil || r.raw == nil {
		return -1
	}
	return r.raw.ContentLength
}

// ContentType reports the media type without its parameters, e.g.
// "application/json" for "application/json; charset=utf-8".
func (r *Response) ContentType() string {
	contentType := r.Header().Get("Content-Type")
	if contentType == "" {
		return ""
	}
	mediaType, _, err := mime.ParseMediaType(contentType)
	if err != nil {
		return contentType
	}
	return mediaType
}

// Request returns the request that produced this response, which after a
// redirect chain is the final one.
func (r *Response) Request() *http.Request {
	if r == nil || r.raw == nil {
		return nil
	}
	return r.raw.Request
}

// Cookies returns the cookies the Set-Cookie headers carried.
func (r *Response) Cookies() []*http.Cookie {
	if r == nil || r.raw == nil {
		return nil
	}
	return r.raw.Cookies()
}

// URL reports the URL that produced this response, after redirects.
func (r *Response) URL() string {
	req := r.Request()
	if req == nil || req.URL == nil {
		return ""
	}
	return req.URL.Redacted()
}

// ------------
// Status helpers
// ------------

// IsSuccess reports whether the status is in the 2xx range.
func (r *Response) IsSuccess() bool {
	code := r.StatusCode()
	return code >= 200 && code < 300
}

// IsRedirect reports whether the status is in the 3xx range. With
// AllowRedirects off these reach the caller instead of being followed.
func (r *Response) IsRedirect() bool {
	code := r.StatusCode()
	return code >= 300 && code < 400
}

// IsClientError reports whether the status is in the 4xx range.
func (r *Response) IsClientError() bool {
	code := r.StatusCode()
	return code >= 400 && code < 500
}

// IsServerError reports whether the status is in the 5xx range.
func (r *Response) IsServerError() bool {
	return r.StatusCode() >= 500
}

// IsError reports whether the status is 4xx or 5xx.
func (r *Response) IsError() bool {
	return r.StatusCode() >= 400
}

// ErrorForStatus turns a non-2xx status into a *StatusError, and returns nil
// for a successful one. The transport only errors when it never got a
// response, so this is what turns "the server said no" into an error value.
func (r *Response) ErrorForStatus() error {
	if r == nil || r.raw == nil {
		return ErrNilResponse
	}
	if r.IsSuccess() {
		return nil
	}

	statusErr := &StatusError{
		StatusCode: r.StatusCode(),
		Status:     r.Status(),
		URL:        r.URL(),
	}
	// A body is best effort here: the status is the error either way.
	if body, err := r.Bytes(); err == nil {
		statusErr.Snippet = snippet(body)
	}
	return statusErr
}

// ------------
// Body
// ------------

// Bytes reads, caches and returns the body, closing it in the process. Later
// calls return the cached copy, so the returned slice must not be modified.
func (r *Response) Bytes() ([]byte, error) {
	if r == nil || r.raw == nil {
		return nil, ErrNilResponse
	}

	r.mtx.Lock()
	defer r.mtx.Unlock()

	if r.buffered {
		return r.body, r.bodyErr
	}
	if r.streamed {
		return nil, ErrBodyConsumed
	}
	r.buffered = true

	if r.raw.Body == nil {
		return nil, nil
	}

	var reader io.Reader = r.raw.Body
	if r.limit >= 0 {
		// One byte past the limit, so "exactly at" and "over" stay distinct.
		reader = io.LimitReader(reader, r.limit+1)
	}

	body, err := readBody(reader, r.raw.ContentLength, r.limit)
	if err != nil {
		r.bodyErr = err
		r.closeLocked(0)
		return nil, err
	}
	if r.limit >= 0 && int64(len(body)) > r.limit {
		r.bodyErr = fmt.Errorf("%w of %d bytes", ErrBodyTooLarge, r.limit)
		// Draining the remainder of an oversized body is not worth the
		// bandwidth, so drop the connection instead of pooling it.
		r.closeLocked(0)
		return nil, r.bodyErr
	}

	r.body = body
	r.closeLocked(maxDrainOnClose)
	return body, nil
}

// readBody reads the whole body. When the server declared its length and that
// fits the limit, it reads into a buffer of exactly that size; io.ReadAll
// would grow one by doubling, copying every time and ending up holding about
// twice the body.
func readBody(r io.Reader, size, limit int64) ([]byte, error) {
	if size <= 0 || (limit >= 0 && size > limit) {
		return io.ReadAll(r)
	}
	buf := make([]byte, size)
	n, err := io.ReadFull(r, buf)
	if err == io.EOF {
		// Nothing arrived although a length was declared: a truncated body,
		// which is how io.ReadAll's caller would have seen it from net/http.
		err = io.ErrUnexpectedEOF
	}
	return buf[:n], err
}

// Text returns the body as a string.
func (r *Response) Text() (string, error) {
	body, err := r.Bytes()
	if err != nil {
		return "", err
	}
	return string(body), nil
}

// JSON decodes the body into v.
func (r *Response) JSON(v any) error {
	body, err := r.Bytes()
	if err != nil {
		return err
	}
	if len(body) == 0 {
		return fmt.Errorf("cannot decode JSON: empty body (status %s)", r.Status())
	}
	if err := json.Unmarshal(body, v); err != nil {
		return fmt.Errorf("cannot decode JSON (status %s, content-type %q): %w",
			r.Status(), r.ContentType(), err)
	}
	return nil
}

// Stream hands the body over for reading, for responses too large to buffer.
// The caller owns it and must close it. If the body was already buffered, a
// reader over the cached copy is returned instead.
func (r *Response) Stream() (io.ReadCloser, error) {
	if r == nil || r.raw == nil {
		return nil, ErrNilResponse
	}

	r.mtx.Lock()
	defer r.mtx.Unlock()

	if r.buffered {
		if r.bodyErr != nil {
			return nil, r.bodyErr
		}
		return io.NopCloser(bytes.NewReader(r.body)), nil
	}
	if r.streamed {
		return nil, ErrBodyConsumed
	}
	if r.raw.Body == nil {
		return io.NopCloser(strings.NewReader("")), nil
	}

	r.streamed = true
	return r.raw.Body, nil
}

// Save copies the body into w without buffering it, and closes it afterwards.
// It ignores MaxResponseBodySize: nothing is held in memory.
func (r *Response) Save(w io.Writer) (int64, error) {
	body, err := r.Stream()
	if err != nil {
		return 0, err
	}
	defer body.Close()

	written, err := io.Copy(w, body)
	if err != nil {
		return written, err
	}
	return written, nil
}

// Discard reads the rest of the body and closes it without keeping it, which
// is what lets the connection go back into the pool. Unlike Close, which gives
// up after 64 KiB, it reads up to MaxResponseBodySize, so a large body still
// returns its connection instead of costing a new dial on the next request. A
// body that declares a length over the limit is closed without being read.
func (r *Response) Discard() error {
	if r == nil || r.raw == nil {
		return nil
	}
	r.mtx.Lock()
	defer r.mtx.Unlock()

	if r.limit >= 0 && r.raw.ContentLength > r.limit {
		return r.closeLocked(0)
	}
	if r.limit < 0 {
		return r.closeLocked(drainAll)
	}
	return r.closeLocked(r.limit)
}

// Close drains a bounded amount of any unread body and closes it. It is safe
// to call more than once, and on a nil Response.
func (r *Response) Close() error {
	if r == nil || r.raw == nil {
		return nil
	}
	r.mtx.Lock()
	defer r.mtx.Unlock()
	return r.closeLocked(maxDrainOnClose)
}

// drainAll tells closeLocked to read the body to the end, however long.
const drainAll = -1

// closeLocked closes the underlying body. It first reads up to drain bytes, or
// everything for drainAll, so the connection can be reused; a body with more
// left than that, or closed with drain 0, is closed outright, which kills the
// connection.
func (r *Response) closeLocked(drain int64) error {
	if r.closed {
		return nil
	}
	r.closed = true
	if r.raw.Body == nil {
		return nil
	}
	// Errors here only mean the connection cannot be reused.
	switch {
	case drain == drainAll:
		io.Copy(io.Discard, r.raw.Body)
	case drain > 0:
		io.CopyN(io.Discard, r.raw.Body, drain)
	}
	return r.raw.Body.Close()
}

// ------------
// Batch helpers
// ------------

// CloseResults closes every response in a batch. Deferring this immediately
// after a batch call is the simplest way to avoid leaking connections when a
// later error returns early.
func CloseResults(results []BatchResult) {
	for _, result := range results {
		result.Response.Close()
	}
}

// FirstError returns the first error in a batch, or nil when every request
// succeeded. It does not inspect status codes; use ErrorsForStatus for that.
func FirstError(results []BatchResult) error {
	for i, result := range results {
		if result.Err != nil {
			return fmt.Errorf("request %d: %w", i, result.Err)
		}
	}
	return nil
}

// Errors collects the transport errors of a batch, preserving order.
func Errors(results []BatchResult) []error {
	var errs []error
	for i, result := range results {
		if result.Err != nil {
			errs = append(errs, fmt.Errorf("request %d: %w", i, result.Err))
		}
	}
	return errs
}

// ------------
// Helpers
// ------------

// snippet trims a body down to something safe to put in an error message.
func snippet(body []byte) string {
	text := strings.TrimSpace(string(body))
	if text == "" {
		return ""
	}
	// Collapse newlines so the message stays on one line.
	text = strings.Join(strings.Fields(text), " ")
	if len(text) > snippetLength {
		return text[:snippetLength] + "..."
	}
	return text
}
