package tests

import (
	requester "jr_requester/jr_requester"
	"net/http"
	"net/http/httptest"
	"testing"
)

// These tests live outside the library package on purpose: they exercise only
// the exported API, which is what a consumer of JrRequester actually sees.

// newTestConn builds a connection with pooling enabled, optionally tuned.
func newTestConn(t *testing.T, useMetrics bool, tune func(*requester.ConnSettings)) *requester.Connection {
	t.Helper()

	opts := []requester.ConnOption{
		requester.WithSettingsFunc(func(s *requester.ConnSettings) {
			s.SetUseKeepAlive(true)
			s.SetUseMetrics(useMetrics)
		}),
	}
	if tune != nil {
		opts = append(opts, requester.WithSettingsFunc(tune))
	}

	conn, err := requester.NewConnection(1, opts...)
	if err != nil {
		t.Fatalf("NewConnection: %v", err)
	}
	return conn
}

// serve starts an httptest server that is shut down when the test ends.
func serve(t *testing.T, handler http.HandlerFunc) *httptest.Server {
	t.Helper()
	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)
	return server
}

// closeBodies releases every response a batch handed back.
func closeBodies(results []requester.BatchResult) {
	for _, r := range results {
		if r.Response != nil {
			r.Response.Close()
		}
	}
}
