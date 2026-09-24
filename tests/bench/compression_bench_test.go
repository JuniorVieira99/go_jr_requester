package bench

import (
	"bytes"
	requester "jr_requester/jr_requester"
	"testing"
)

// compressible is repetitive JSON-like text, closer to a real API body than
// the single-byte filler, which compresses unrealistically well.
var compressible = bytes.Repeat([]byte(`{"id":12345,"name":"example item","tags":["alpha","beta"],"score":0.3333}`), 16<<10/72)

// BenchmarkCompress measures the handler's gzip and zlib helpers. MB/s is the
// uncompressed throughput.
func BenchmarkCompress(b *testing.B) {
	handler := requester.NewCompressionHandler()
	for _, algo := range []requester.CompressionType{requester.GZIP, requester.ZLIB} {
		b.Run(string(algo), func(b *testing.B) {
			b.ReportAllocs()
			b.SetBytes(int64(len(compressible)))
			var out []byte
			for b.Loop() {
				var err error
				out, err = handler.Compress(compressible, algo)
				if err != nil {
					b.Fatal(err)
				}
			}
			b.ReportMetric(float64(len(compressible))/float64(len(out)), "ratio")
		})
	}
}

// BenchmarkDecompress measures the matching decompression.
func BenchmarkDecompress(b *testing.B) {
	handler := requester.NewCompressionHandler()
	for _, algo := range []requester.CompressionType{requester.GZIP, requester.ZLIB} {
		compressed, err := handler.Compress(compressible, algo)
		if err != nil {
			b.Fatal(err)
		}
		b.Run(string(algo), func(b *testing.B) {
			b.ReportAllocs()
			b.SetBytes(int64(len(compressible)))
			for b.Loop() {
				if _, err := handler.Decompress(compressed, algo); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
