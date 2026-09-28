package bench

import (
	"context"
	"encoding/json"
	"fmt"
	requester "github.com/JuniorVieira99/go_jr_requester/jr_requester"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
)

// BenchmarkResponseRead compares the ways of consuming a body. Discard drains
// up to MaxResponseBodySize, so unlike Close it keeps the connection for the
// next request even at 1 MiB.
func BenchmarkResponseRead(b *testing.B) {
	server := newServer(b)

	readers := []struct {
		name string
		read func(*requester.Response) error
	}{
		{"Bytes", func(r *requester.Response) error {
			_, err := r.Bytes()
			return err
		}},
		{"Stream", func(r *requester.Response) error {
			body, err := r.Stream()
			if err != nil {
				return err
			}
			defer body.Close()
			_, err = io.Copy(io.Discard, body)
			return err
		}},
		{"Save", func(r *requester.Response) error {
			_, err := r.Save(io.Discard)
			return err
		}},
		{"Discard", func(r *requester.Response) error {
			return r.Discard()
		}},
	}

	for _, size := range []int{1 << 10, 64 << 10, 1 << 20} {
		for _, reader := range readers {
			b.Run(reader.name+"/"+sizeName(size), func(b *testing.B) {
				conn := newConn(b)
				rest := conn.GetRestHandler()
				url := sizedURL(server, size)
				ctx := context.Background()

				b.ReportAllocs()
				b.SetBytes(int64(size))
				for b.Loop() {
					resp, err := rest.Get(ctx, url, nil)
					if err != nil {
						b.Fatal(err)
					}
					if err := reader.read(resp); err != nil {
						b.Fatal(err)
					}
				}
			})
		}
	}
}

// benchItem is the element of the JSON document BenchmarkResponseJSON decodes.
type benchItem struct {
	ID    int      `json:"id"`
	Name  string   `json:"name"`
	Tags  []string `json:"tags"`
	Score float64  `json:"score"`
}

// BenchmarkResponseJSON measures Response.JSON on a list of objects.
func BenchmarkResponseJSON(b *testing.B) {
	for _, n := range []int{10, 1000} {
		items := make([]benchItem, n)
		for i := range items {
			items[i] = benchItem{ID: i, Name: "item", Tags: []string{"a", "b"}, Score: float64(i) / 3}
		}
		doc, err := json.Marshal(items)
		if err != nil {
			b.Fatal(err)
		}

		b.Run(fmt.Sprintf("items=%d", n), func(b *testing.B) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				w.Write(doc)
			}))
			b.Cleanup(server.Close)

			conn := newConn(b)
			rest := conn.GetRestHandler()
			ctx := context.Background()

			b.ReportAllocs()
			b.SetBytes(int64(len(doc)))
			for b.Loop() {
				resp, err := rest.Get(ctx, server.URL, nil)
				if err != nil {
					b.Fatal(err)
				}
				var out []benchItem
				if err := resp.JSON(&out); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
