package tests

import (
	"context"
	"fmt"
	requester "jr_requester/jr_requester"
	"net/http"
	"testing"
	"time"
)

// TestConnection exercises the public API against a real host. It needs
// network access, so it is skipped under -short.
func TestConnection(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping the network-dependent test under -short")
	}

	conn, err := requester.NewConnection(1)
	if err != nil {
		t.Fatal(err)
	}
	if conn == nil {
		t.Fatal("expected connection")
	}
	defer conn.Close()

	resp, err := conn.GetRestHandler().Get(context.TODO(), "https://jsonplaceholder.typicode.com/posts/1", nil)
	if err != nil {
		t.Fatal(err)
	}
	if resp == nil {
		t.Fatal("expected response")
	}
	// Closing the response returns its connection to the pool. Doing it in a
	// defer means an early t.Fatal below cannot leak it.
	defer resp.Close()

	if resp.StatusCode() != 200 {
		t.Fatalf("expected status code 200, got %d", resp.StatusCode())
	}
	t.Logf("Response: %v", resp)

	// The body can be decoded straight into a struct; Close stays safe after.
	var post struct {
		ID     int    `json:"id"`
		UserID int    `json:"userId"`
		Title  string `json:"title"`
	}
	if err := resp.JSON(&post); err != nil {
		t.Fatal(err)
	}
	if post.ID != 1 {
		t.Fatalf("expected post 1, got %d", post.ID)
	}
	t.Logf("Post: %+v", post)
}

// TestBatch exercises the concurrent batch path end to end.
func TestBatch(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping the network-dependent test under -short")
	}

	conn, err := requester.NewConnection(2,
		requester.WithAsync(),
		requester.WithKeepAlive(8, 16),
		requester.WithMetrics(),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	urls := []string{
		"https://jsonplaceholder.typicode.com/posts/1",
		"https://jsonplaceholder.typicode.com/posts/2",
		"https://jsonplaceholder.typicode.com/posts/3",
	}

	results := conn.GetRestHandler().BatchGet(context.Background(), urls, nil)
	defer requester.CloseResults(results)

	if err := requester.FirstError(results); err != nil {
		t.Fatal(err)
	}
	for i, result := range results {
		if err := result.Response.ErrorForStatus(); err != nil {
			t.Fatalf("result %d: %v", i, err)
		}
	}

	metrics := conn.GetMetrics()
	if metrics.TotalRequests != 3 || metrics.SuccessfulRequests != 3 {
		t.Fatalf("metrics = %+v", metrics)
	}
	t.Logf("alive=%d idle=%d closed=%d", metrics.AliveConnections, metrics.IdleConnections, metrics.ClosedConnections)
}

func TestCompressionGzip(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping the network-dependent test under -short")
	}

	conn, err := requester.NewConnection(2,
		requester.WithAsync(),
		requester.WithKeepAlive(8, 16),
		requester.WithApiKey("Authorization", "API-KEY"),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	res, err := conn.GetRestHandler().Get(context.Background(), "https://jsonplaceholder.typicode.com/posts/1", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer res.Close()

	compressed, err := conn.GetCompressionHandler().CompressFromResponse(res, requester.GZIP)
	if err != nil {
		t.Fatal(err)
	}

	body, err := res.Bytes()
	if err != nil {
		t.Fatal(err)
	}

	if len(compressed) >= len(body) {
		t.Fatalf("compressed size %d is not smaller than original size %d", len(compressed), len(body))
	}

	decompressed, err := conn.GetCompressionHandler().Decompress(compressed, requester.GZIP)
	if err != nil {
		t.Fatal(err)
	}

	if string(decompressed) != string(body) {
		t.Fatalf("decompressed data does not match original data")
	}
}

func TestCompressionZlib(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping the network-dependent test under -short")
	}

	conn, err := requester.NewConnection(2,
		requester.WithAsync(),
		requester.WithKeepAlive(8, 16),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	compression := conn.GetCompressionHandler()

	res, err := conn.GetRestHandler().Get(context.Background(), "https://jsonplaceholder.typicode.com/posts/1", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer res.Close()

	compressed, err := compression.CompressFromResponse(res, requester.ZLIB)
	if err != nil {
		t.Fatal(err)
	}

	body, err := res.Bytes()
	if err != nil {
		t.Fatal(err)
	}

	if len(compressed) >= len(body) {
		t.Fatalf("compressed size %d is not smaller than original size %d", len(compressed), len(body))
	}

	decompressed, err := compression.Decompress(compressed, requester.ZLIB)
	if err != nil {
		t.Fatal(err)
	}

	if string(decompressed) != string(body) {
		t.Fatalf("decompressed data does not match original data")
	}
}

func TestMiddlewares(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping the network-dependent test under -short")
	}

	printing := func() requester.Middleware {
		return func(req *http.Request, next requester.RoundTripFunc) (*http.Response, error) {
			var toPrint string
			start := time.Now()
			resp, err := next(req)

			toPrint += "Request: " + req.Method + " " + req.URL.String() + "\n"
			if resp != nil {
				toPrint += "Response: " + resp.Status + "\n"
			}
			toPrint += "Duration: " + time.Since(start).String() + "\n"

			if err != nil {
				toPrint += "Error: " + err.Error() + "\n"
			}
			fmt.Print(toPrint)
			return resp, err
		}
	}

	conn, err := requester.NewConnection(2,
		requester.WithAsync(),
		requester.WithKeepAlive(8, 16),
		requester.WithMiddleware(printing()),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Shutdown()

	resp, err := conn.GetRestHandler().Get(context.Background(), "https://jsonplaceholder.typicode.com/posts/1", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Close()

	if resp.StatusCode() != 200 {
		t.Fatalf("expected status code 200, got %d", resp.StatusCode())
	}
}
