package main

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

func wsURL(httpURL, query string) string {
	return "ws" + strings.TrimPrefix(httpURL, "http") + "/ws?" + query
}

func dialWS(t *testing.T, httpURL, query string) *websocket.Conn {
	t.Helper()
	c, _, err := websocket.DefaultDialer.Dial(wsURL(httpURL, query), nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = c.Close() })
	return c
}

func waitServerData(t *testing.T, p *pipe) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		p.mu.RLock()
		ready := p.serverData != nil
		p.mu.RUnlock()
		if ready {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("timed out waiting for serverData")
}

// wsPair returns a relay-side conn and the peer that can ReadMessage what
// the relay writes.
func wsPair(t *testing.T) (*conn, *websocket.Conn) {
	t.Helper()
	serverCh := make(chan *websocket.Conn, 1)
	done := make(chan struct{})
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ws, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			t.Error(err)
			return
		}
		serverCh <- ws
		<-done
	}))
	t.Cleanup(func() {
		close(done)
		s.Close()
	})

	u := "ws" + strings.TrimPrefix(s.URL, "http")
	peer, _, err := websocket.DefaultDialer.Dial(u, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = peer.Close() })

	ws := <-serverCh
	t.Cleanup(func() { _ = ws.Close() })
	return newConn(ws), peer
}

// TestHandoverSwapsPendingBuffer verifies that handleServerData swaps in a
// fresh pending buffer when it takes over.
//
// handleClient observes p.serverData under RLock and may release the lock
// before it pushes to p.pending. If handleServerData drains p.pending in
// place between the unlock and the push, the frame ends up in a buffer that
// will never be drained again.
func TestHandoverSwapsPendingBuffer(t *testing.T) {
	reg := newRegistry(64)
	ts := httptest.NewServer(&relayHandler{reg: reg})
	t.Cleanup(ts.Close)

	const serverId = "sid"
	const connectionId = "cid"

	sess, ok := reg.get(serverId)
	if !ok {
		t.Fatal("failed to create session")
	}
	p := sess.getOrCreatePipe(connectionId)

	p.pending.push(websocket.BinaryMessage, []byte("pre-handover"))
	pre := p.pending

	c := dialWS(t, ts.URL, "serverId="+serverId+"&role=server&connectionId="+connectionId+"&v=2")
	waitServerData(t, p)

	p.mu.RLock()
	pending := p.pending
	p.mu.RUnlock()
	if pending == pre {
		t.Fatal("p.pending was not swapped during handover — frames pushed " +
			"after the handover would be stranded in the drained buffer")
	}

	c.SetReadDeadline(time.Now().Add(2 * time.Second))
	_, got, err := c.ReadMessage()
	if err != nil {
		t.Fatalf("pre-handover frame was not flushed to the daemon: %v", err)
	}
	if !bytes.Equal(got, []byte("pre-handover")) {
		t.Fatalf("flushed frame = %q, want %q", got, "pre-handover")
	}
}

// TestSlowPathRechecksServerData verifies that the client slow path re-reads
// p.serverData under the write lock before deciding to buffer.
//
// The pre-fix code did:
//
//	RLock; srv := p.serverData; RUnlock
//	if srv == nil { p.pending.push(...) }
//
// The window between RUnlock and push() lets handleServerData set
// serverData and drain pending in-between, stranding the push.
func TestSlowPathRechecksServerData(t *testing.T) {
	relay, peer := wsPair(t)
	p := &pipe{pending: newFrameBuffer(64), serverData: relay}

	payload := []byte("e2ee_hello")
	if err := p.sendOrBuffer(websocket.BinaryMessage, payload); err != nil {
		t.Fatalf("sendOrBuffer: %v", err)
	}

	if !p.pending.isEmpty() {
		t.Fatal("frame ended up in pending despite serverData being set")
	}

	peer.SetReadDeadline(time.Now().Add(2 * time.Second))
	_, got, err := peer.ReadMessage()
	if err != nil {
		t.Fatalf("slow path did not send the frame to the daemon: %v", err)
	}
	if !bytes.Equal(got, payload) {
		t.Fatalf("got %q, want %q", got, payload)
	}
}
