package main

import (
	"bufio"
	"bytes"
	"io"
	"net"
	"net/http"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

// serveWithTimeouts runs the relay's HTTP server with its timeouts shortened
// to d and returns the listen address.
func serveWithTimeouts(t *testing.T, d time.Duration) string {
	t.Helper()
	srv := newServer("", &relayHandler{reg: newRegistry(10)})
	if srv.ReadHeaderTimeout <= 0 || srv.IdleTimeout <= 0 {
		t.Fatalf("newServer: ReadHeaderTimeout=%v IdleTimeout=%v, want both set", srv.ReadHeaderTimeout, srv.IdleTimeout)
	}
	srv.ReadHeaderTimeout, srv.IdleTimeout = d, d
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	go func() { _ = srv.Serve(ln) }()
	t.Cleanup(func() { _ = srv.Close() })
	return ln.Addr().String()
}

// serverClosesWithin reports whether the server closes c, read through r,
// within d.
func serverClosesWithin(c net.Conn, r io.Reader, d time.Duration) bool {
	_ = c.SetReadDeadline(time.Now().Add(d))
	_, err := io.Copy(io.Discard, r)
	return err == nil // EOF; hitting the deadline returns a timeout error
}

func TestServerDropsSlowHeaders(t *testing.T) {
	addr := serveWithTimeouts(t, 100*time.Millisecond)
	c, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	// Headers that never finish.
	if _, err := c.Write([]byte("GET /health HTTP/1.1\r\nHost: relay\r\n")); err != nil {
		t.Fatal(err)
	}
	if !serverClosesWithin(c, c, 2*time.Second) {
		t.Fatal("connection with unfinished headers was not closed")
	}
}

func TestServerDropsIdleKeepAlive(t *testing.T) {
	addr := serveWithTimeouts(t, 100*time.Millisecond)
	c, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	if _, err := c.Write([]byte("GET /health HTTP/1.1\r\nHost: relay\r\n\r\n")); err != nil {
		t.Fatal(err)
	}
	br := bufio.NewReader(c)
	resp, err := http.ReadResponse(br, nil)
	if err != nil {
		t.Fatal(err)
	}
	_, _ = io.Copy(io.Discard, resp.Body)
	_ = resp.Body.Close()
	if !serverClosesWithin(c, br, 2*time.Second) {
		t.Fatal("idle keep-alive connection was not closed")
	}
}

func TestServerTimeoutsSpareWebSockets(t *testing.T) {
	addr := serveWithTimeouts(t, 100*time.Millisecond)
	ws, _, err := websocket.DefaultDialer.Dial("ws://"+addr+"/ws?serverId=s1&role=server&v=2", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer ws.Close()

	_ = ws.SetReadDeadline(time.Now().Add(2 * time.Second))
	if _, _, err := ws.ReadMessage(); err != nil { // initial sync
		t.Fatal(err)
	}
	time.Sleep(500 * time.Millisecond) // several header and idle timeouts
	if err := ws.WriteMessage(websocket.TextMessage, []byte(`{"type":"ping"}`)); err != nil {
		t.Fatal(err)
	}
	_, msg, err := ws.ReadMessage()
	if err != nil || !bytes.Contains(msg, []byte(`"pong"`)) {
		t.Fatalf("ping after idling: msg=%q err=%v", msg, err)
	}
}
