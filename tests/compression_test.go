package tests

import (
	"bytes"
	"compress/gzip"
	"context"
	"errors"
	"fmt"
	requester "jr_requester/jr_requester"
	"net/http"
	"slices"
	"strings"
	"sync"
	"testing"
)

func TestCompressRoundTrip(t *testing.T) {
	comp := requester.NewCompressionHandler()
	// Repetitive input, so compression is guaranteed to shrink it.
	payload := []byte(strings.Repeat("JrRequester compresses this. ", 64))

	for _, cType := range []requester.CompressionType{requester.GZIP, requester.ZLIB} {
		packed, err := comp.Compress(payload, cType)
		if err != nil {
			t.Fatalf("%s: %v", cType, err)
		}
		if len(packed) >= len(payload) {
			t.Fatalf("%s: packed %d bytes is not smaller than %d", cType, len(packed), len(payload))
		}

		plain, err := comp.Decompress(packed, cType)
		if err != nil {
			t.Fatalf("%s: %v", cType, err)
		}
		if !bytes.Equal(plain, payload) {
			t.Fatalf("%s: round trip changed the data", cType)
		}
	}

	if _, err := comp.Compress(payload, requester.CompressionType("br")); err == nil {
		t.Fatal("expected an error for an unsupported compression type")
	}
	if _, err := comp.Decompress(payload, requester.CompressionType("br")); err == nil {
		t.Fatal("expected an error for an unsupported decompression type")
	}
	if _, err := comp.Decompress([]byte("not compressed"), requester.GZIP); err == nil {
		t.Fatal("expected an error for malformed gzip data")
	}

	// Both counters advanced once per algorithm above; NONE is a pass-through
	// and is deliberately not counted.
	if _, err := comp.Compress(payload, requester.NONE); err != nil {
		t.Fatalf("NONE compress: %v", err)
	}
	stats := comp.Stats()
	if stats.Compressions != 2 || stats.Decompressions != 2 {
		t.Fatalf("stats = %+v", stats)
	}
	if stats.CompressedBytes == 0 || stats.DecompressedBytes == 0 {
		t.Fatalf("byte counters not recorded: %+v", stats)
	}

	comp.ResetStats()
	if comp.Stats() != (requester.CompressionStats{}) {
		t.Fatalf("ResetStats left %+v", comp.Stats())
	}
}

func TestCompressFromResponse(t *testing.T) {
	payload := strings.Repeat("body ", 256)
	server := serve(t, func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(payload))
	})

	comp := requester.NewCompressionHandler()
	c := newTestConn(t, false, nil)
	resp, err := c.GetRestHandler().Get(context.Background(), server.URL, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Close()

	packed, err := comp.CompressFromResponse(resp, requester.GZIP)
	if err != nil {
		t.Fatal(err)
	}
	if len(packed) >= len(payload) {
		t.Fatalf("packed %d bytes is not smaller than %d", len(packed), len(payload))
	}

	// The response body is cached, so it is still readable afterwards.
	body, err := resp.Bytes()
	if err != nil {
		t.Fatal(err)
	}
	plain, err := comp.Decompress(packed, requester.GZIP)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(plain, body) {
		t.Fatal("decompressed data does not match the response body")
	}

	var nilResp *requester.Response
	if _, err := comp.CompressFromResponse(nilResp, requester.GZIP); err == nil {
		t.Fatal("expected an error for a nil response")
	}
	if _, err := comp.DecompressFromResponse(nilResp); err == nil {
		t.Fatal("expected an error for a nil response")
	}
}

// TestCompressionModes pins who decodes the body in each mode. The handler
// owns this policy now: net/http only decompresses a body when it added
// Accept-Encoding itself, so the header the handler injects is what decides.
func TestCompressionModes(t *testing.T) {
	const payload = `{"hello":"world"}`
	var accepted []string
	server := serve(t, func(w http.ResponseWriter, r *http.Request) {
		accepted = append(accepted, r.Header.Get("Accept-Encoding"))
		if r.Header.Get("Accept-Encoding") == "identity" {
			w.Write([]byte(payload))
			return
		}
		w.Header().Set("Content-Encoding", "gzip")
		writer := gzip.NewWriter(w)
		writer.Write([]byte(payload))
		writer.Close()
	})

	// Transport mode is the default and net/http unpacks the body itself.
	c := newTestConn(t, false, nil)
	comp := c.GetCompressionHandler()
	if comp == nil {
		t.Fatal("GetCompressionHandler returned nil")
	}
	if comp.Mode() != requester.CompressionTransport {
		t.Fatalf("default mode = %v, want transport", comp.Mode())
	}

	resp, err := c.GetRestHandler().Get(context.Background(), server.URL, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Close()
	if got := resp.Header().Get("Content-Encoding"); got != "" {
		t.Fatalf("Content-Encoding = %q, expected the transport to strip it", got)
	}
	if !resp.Raw().Uncompressed {
		t.Fatal("expected the transport to have decompressed the body")
	}
	if _, err := comp.DecompressFromResponse(resp); !errors.Is(err, requester.ErrNoContentEncoding) {
		t.Fatalf("err = %v, want ErrNoContentEncoding", err)
	}
	if text, err := resp.Text(); err != nil || text != payload {
		t.Fatalf("Text() = %q, %v", text, err)
	}

	// Manual mode keeps the body encoded for the handler to decode.
	if err := comp.SetMode(requester.CompressionManual); err != nil {
		t.Fatal(err)
	}
	packed, err := c.GetRestHandler().Get(context.Background(), server.URL, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer packed.Close()
	if got := packed.Header().Get("Content-Encoding"); got != "gzip" {
		t.Fatalf("Content-Encoding = %q, want gzip", got)
	}
	plain, err := comp.DecompressFromResponse(packed)
	if err != nil {
		t.Fatal(err)
	}
	if string(plain) != payload {
		t.Fatalf("decompressed = %q, want %q", plain, payload)
	}

	// Off mode asks for no compression at all.
	if err := comp.SetMode(requester.CompressionOff); err != nil {
		t.Fatal(err)
	}
	identity, err := c.GetRestHandler().Get(context.Background(), server.URL, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer identity.Close()
	if text, err := identity.Text(); err != nil || text != payload {
		t.Fatalf("Text() = %q, %v", text, err)
	}

	if want := []string{"gzip", "gzip", "identity"}; !slices.Equal(accepted, want) {
		t.Fatalf("Accept-Encoding seen = %v, want %v", accepted, want)
	}

	// An unknown mode is rejected rather than silently applied.
	if err := comp.SetMode(requester.CompressionMode(42)); err == nil {
		t.Fatal("expected an error for an unknown mode")
	}
	if comp.Mode() != requester.CompressionOff {
		t.Fatalf("a rejected SetMode changed the mode to %v", comp.Mode())
	}
}

// TestCompressionModeRespectsCallerHeader checks an explicit Accept-Encoding
// always wins over the handler's mode.
func TestCompressionModeRespectsCallerHeader(t *testing.T) {
	var accepted string
	server := serve(t, func(w http.ResponseWriter, r *http.Request) {
		accepted = r.Header.Get("Accept-Encoding")
	})

	c := newTestConn(t, false, nil)
	if err := c.GetCompressionHandler().SetMode(requester.CompressionOff); err != nil {
		t.Fatal(err)
	}

	resp, err := c.GetRestHandler().Get(context.Background(), server.URL, map[string]string{
		"Accept-Encoding": "gzip",
	})
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Close()

	if accepted != "gzip" {
		t.Fatalf("Accept-Encoding = %q, the caller header should win", accepted)
	}
}

func TestDecompressFromResponseUnsupportedEncoding(t *testing.T) {
	server := serve(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Encoding", "br")
		w.Write([]byte("whatever"))
	})

	comp := requester.NewCompressionHandler()
	c := newTestConn(t, false, nil)
	resp, err := c.GetRestHandler().Get(context.Background(), server.URL, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Close()

	if _, err := comp.DecompressFromResponse(resp); err == nil {
		t.Fatal("expected an error for an unsupported content encoding")
	}
}

// TestPooledCodecsStayCorrect hammers the pooled writers and readers from many
// goroutines, with malformed input mixed in, so a codec that comes back from
// the pool in a bad state or shared between calls shows up as corrupt output.
func TestPooledCodecsStayCorrect(t *testing.T) {
	comp := requester.NewCompressionHandler()
	var wg sync.WaitGroup
	for g := range 16 {
		wg.Go(func() {
			for i := range 50 {
				payload := []byte(strings.Repeat(fmt.Sprintf("goroutine %d item %d ", g, i), 20+i))
				for _, cType := range []requester.CompressionType{requester.GZIP, requester.ZLIB} {
					// A failed decode must not poison the reader it used.
					if _, err := comp.Decompress([]byte("garbage"), cType); err == nil {
						t.Errorf("%s: garbage decoded without error", cType)
						return
					}
					packed, err := comp.Compress(payload, cType)
					if err != nil {
						t.Errorf("%s: %v", cType, err)
						return
					}
					plain, err := comp.Decompress(packed, cType)
					if err != nil || !bytes.Equal(plain, payload) {
						t.Errorf("%s: round trip broke: err = %v", cType, err)
						return
					}
				}
			}
		})
	}
	wg.Wait()
}
