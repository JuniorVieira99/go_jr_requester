package tests

import (
	requester "github.com/JuniorVieira99/go_jr_requester/jr_requester"
	"net"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
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

// serveCountingConns is serve plus a count of the TCP connections the server
// accepts. ConnState is set before Start, because the accept loop reads it.
func serveCountingConns(t *testing.T, handler http.HandlerFunc) (*httptest.Server, *atomic.Int64) {
	t.Helper()
	var conns atomic.Int64
	server := httptest.NewUnstartedServer(handler)
	server.Config.ConnState = func(_ net.Conn, state http.ConnState) {
		if state == http.StateNew {
			conns.Add(1)
		}
	}
	server.Start()
	t.Cleanup(server.Close)
	return server, &conns
}

// closeBodies releases every response a batch handed back.
func closeBodies(results []requester.BatchResult) {
	for _, r := range results {
		if r.Response != nil {
			r.Response.Close()
		}
	}
}
