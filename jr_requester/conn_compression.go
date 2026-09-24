package jr_requester

import (
	"bytes"
	"compress/gzip"
	"compress/zlib"
	"errors"
	"fmt"
	"io"
	"net/http"
	"sync"
)

// ------------
// Enums
// ------------

type CompressionType string

const (
	NONE CompressionType = "none"
	GZIP CompressionType = "gzip"
	ZLIB CompressionType = "zlib"
)

// CompressionMode decides who handles compression on the wire.
type CompressionMode int

const (
	// CompressionTransport lets net/http negotiate and decode, which is the
	// default. Bodies arrive already decompressed, so Response.Bytes returns
	// plain data and DecompressFromResponse has nothing left to do.
	CompressionTransport CompressionMode = iota

	// CompressionManual asks for gzip but leaves the body encoded for
	// DecompressFromResponse. net/http only decodes a body when it added
	// Accept-Encoding itself, so setting the header explicitly is what hands
	// the body over untouched.
	CompressionManual

	// CompressionOff asks the server not to compress at all.
	CompressionOff
)

func (m CompressionMode) String() string {
	switch m {
	case CompressionTransport:
		return "transport"
	case CompressionManual:
		return "manual"
	case CompressionOff:
		return "off"
	default:
		return "unknown"
	}
}

// ------------
// Errors
// ------------

var (
	// ErrUnsupportedCompression is returned for an algorithm this handler
	// does not implement.
	ErrUnsupportedCompression = errors.New("unsupported compression type")
	// ErrNoContentEncoding is returned when a response carries no
	// Content-Encoding header to decode by.
	ErrNoContentEncoding = errors.New("no Content-Encoding header found")
)

// ------------
// Structs
// ------------

type ICompression interface {
	Compress(data []byte, cType CompressionType) ([]byte, error)
	Decompress(data []byte, cType CompressionType) ([]byte, error)
	CompressFromResponse(resp *Response, cType CompressionType) ([]byte, error)
	DecompressFromResponse(resp *Response) ([]byte, error)
}

// CompressionStats is a snapshot of a handler's counters.
type CompressionStats struct {
	Compressions      int64
	Decompressions    int64
	CompressedBytes   int64
	DecompressedBytes int64
}

// CompressionHandler owns a connection's compression policy and provides the
// gzip/zlib helpers. Reach a connection's handler through
// Connection.GetCompressionHandler; NewCompressionHandler builds a standalone
// one for the Compress/Decompress helpers alone.
type CompressionHandler struct {
	mu   sync.Mutex
	mode CompressionMode

	compressions      int64
	decompressions    int64
	compressedBytes   int64
	decompressedBytes int64
}

// ------------
// Constructors
// ------------

// NewCompressionHandler builds a handler detached from any connection. Its
// mode affects nothing; only the Compress/Decompress helpers apply.
func NewCompressionHandler() *CompressionHandler {
	return &CompressionHandler{mode: CompressionTransport}
}

// ------------
// Policy
// ------------

// Mode reports who currently handles compression.
func (c *CompressionHandler) Mode() CompressionMode {
	if c == nil {
		return CompressionTransport
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.mode
}

// SetMode changes who handles compression for subsequent requests. It is safe
// to call at any point; requests already in flight keep the previous mode.
func (c *CompressionHandler) SetMode(mode CompressionMode) error {
	if c == nil {
		return errors.New("nil compression handler")
	}
	switch mode {
	case CompressionTransport, CompressionManual, CompressionOff:
	default:
		return fmt.Errorf("unknown compression mode %d", int(mode))
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	c.mode = mode
	return nil
}

// applyTo sets Accept-Encoding to match the mode. A header the caller already
// set always wins, and transport mode sets nothing so net/http can negotiate.
func (c *CompressionHandler) applyTo(req *http.Request) {
	if c == nil || req == nil {
		return
	}

	mode := c.Mode()
	if mode == CompressionTransport {
		return
	}
	if req.Header == nil {
		req.Header = make(http.Header)
	}
	if req.Header.Get("Accept-Encoding") != "" {
		return
	}

	switch mode {
	case CompressionManual:
		req.Header.Set("Accept-Encoding", string(GZIP))
	case CompressionOff:
		req.Header.Set("Accept-Encoding", "identity")
	}
}

// ------------
// Stats
// ------------

// Stats returns a snapshot of the counters, safe to read while the handler is
// in use.
func (c *CompressionHandler) Stats() CompressionStats {
	if c == nil {
		return CompressionStats{}
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return CompressionStats{
		Compressions:      c.compressions,
		Decompressions:    c.decompressions,
		CompressedBytes:   c.compressedBytes,
		DecompressedBytes: c.decompressedBytes,
	}
}

// ResetStats zeroes the counters.
func (c *CompressionHandler) ResetStats() {
	if c == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.compressions, c.decompressions = 0, 0
	c.compressedBytes, c.decompressedBytes = 0, 0
}

func (c *CompressionHandler) addCompressionStats(compressedBytes int64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.compressions++
	c.compressedBytes += compressedBytes
}

func (c *CompressionHandler) addDecompressionStats(decompressedBytes int64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.decompressions++
	c.decompressedBytes += decompressedBytes
}

// ------------
// Helpers
// ------------

// The codecs are pooled. A gzip or zlib writer allocates about 800 KB of
// compressor state and a reader about 40 KB of window, so building one per
// call cost far more than the data being compressed. Each is Reset onto the
// next call's buffer, and sync.Pool drops idle ones at garbage collection.
var (
	gzipWriters = sync.Pool{New: func() any { return gzip.NewWriter(io.Discard) }}
	zlibWriters = sync.Pool{New: func() any { return zlib.NewWriter(io.Discard) }}
	gzipReaders sync.Pool // *gzip.Reader; needs a valid header to build, so no New
	zlibReaders sync.Pool // zlib's io.ReadCloser, which is also a zlib.Resetter
)

// resettableWriter is what both gzip.Writer and zlib.Writer implement.
type resettableWriter interface {
	io.WriteCloser
	Reset(io.Writer)
}

// compressWith runs data through a pooled writer. The writer is pointed back
// at io.Discard before it returns to the pool, so it does not keep the
// caller's output alive.
func (c *CompressionHandler) compressWith(pool *sync.Pool, data []byte) ([]byte, error) {
	var buf bytes.Buffer
	buf.Grow(len(data) / 2)

	w := pool.Get().(resettableWriter)
	w.Reset(&buf)
	defer func() {
		w.Reset(io.Discard)
		pool.Put(w)
	}()

	if _, err := w.Write(data); err != nil {
		return nil, err
	}
	if err := w.Close(); err != nil {
		return nil, err
	}

	packed := buf.Bytes()
	c.addCompressionStats(int64(len(packed)))
	return packed, nil
}

func (c *CompressionHandler) compressToGzip(data []byte) ([]byte, error) {
	return c.compressWith(&gzipWriters, data)
}

func (c *CompressionHandler) compressToZlib(data []byte) ([]byte, error) {
	return c.compressWith(&zlibWriters, data)
}

// readAllCounted drains a decompressor and records the decoded size.
func (c *CompressionHandler) readAllCounted(r io.Reader) ([]byte, error) {
	plain, err := io.ReadAll(r)
	if err != nil {
		return nil, err
	}
	c.addDecompressionStats(int64(len(plain)))
	return plain, nil
}

func (c *CompressionHandler) decompressFromGzip(data []byte) ([]byte, error) {
	src := bytes.NewReader(data)
	gzipReader, ok := gzipReaders.Get().(*gzip.Reader)
	if ok {
		if err := gzipReader.Reset(src); err != nil {
			// A failed Reset leaves the reader reusable; the next Reset
			// starts it over.
			gzipReaders.Put(gzipReader)
			return nil, err
		}
	} else {
		var err error
		if gzipReader, err = gzip.NewReader(src); err != nil {
			return nil, err
		}
	}
	defer gzipReaders.Put(gzipReader)
	return c.readAllCounted(gzipReader)
}

func (c *CompressionHandler) decompressFromZlib(data []byte) ([]byte, error) {
	src := bytes.NewReader(data)
	zlibReader, ok := zlibReaders.Get().(io.ReadCloser)
	if ok {
		if err := zlibReader.(zlib.Resetter).Reset(src, nil); err != nil {
			zlibReaders.Put(zlibReader)
			return nil, err
		}
	} else {
		var err error
		if zlibReader, err = zlib.NewReader(src); err != nil {
			return nil, err
		}
	}
	defer zlibReaders.Put(zlibReader)
	return c.readAllCounted(zlibReader)
}

// ------------
// Methods
// ------------

// Compress encodes data with the given algorithm. NONE passes the data
// through untouched and is not counted, since nothing was compressed.
func (c *CompressionHandler) Compress(data []byte, cType CompressionType) ([]byte, error) {
	switch cType {
	case GZIP:
		return c.compressToGzip(data)
	case ZLIB:
		return c.compressToZlib(data)
	case NONE:
		return data, nil
	default:
		return nil, fmt.Errorf("%w: %q", ErrUnsupportedCompression, cType)
	}
}

// Decompress decodes data with the given algorithm. NONE passes the data
// through untouched.
func (c *CompressionHandler) Decompress(data []byte, cType CompressionType) ([]byte, error) {
	switch cType {
	case GZIP:
		return c.decompressFromGzip(data)
	case ZLIB:
		return c.decompressFromZlib(data)
	case NONE:
		return data, nil
	default:
		return nil, fmt.Errorf("%w: %q", ErrUnsupportedCompression, cType)
	}
}

// CompressFromResponse reads the response body and compresses it. Response
// caches the body, so it stays readable afterwards.
func (c *CompressionHandler) CompressFromResponse(resp *Response, cType CompressionType) ([]byte, error) {
	if resp == nil || resp.raw == nil {
		return nil, ErrNilResponse
	}

	body, err := resp.Bytes()
	if err != nil {
		return nil, err
	}
	return c.Compress(body, cType)
}

// DecompressFromResponse decodes a body that is still encoded, choosing the
// algorithm from Content-Encoding.
//
// This needs CompressionManual mode. Under CompressionTransport net/http has
// already decoded the body and stripped the header, so there is nothing left
// to decode and this reports ErrNoContentEncoding.
func (c *CompressionHandler) DecompressFromResponse(resp *Response) ([]byte, error) {
	if resp == nil || resp.raw == nil {
		return nil, ErrNilResponse
	}

	contentEncoding := resp.Header().Get("Content-Encoding")
	if contentEncoding == "" {
		return nil, ErrNoContentEncoding
	}

	body, err := resp.Bytes()
	if err != nil {
		return nil, err
	}

	switch contentEncoding {
	case "gzip":
		return c.decompressFromGzip(body)
	case "deflate":
		return c.decompressFromZlib(body)
	default:
		return nil, fmt.Errorf("%w: %q", ErrUnsupportedCompression, contentEncoding)
	}
}
