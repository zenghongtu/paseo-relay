package main

import (
	"bytes"
	"errors"
	"fmt"
	"math/rand/v2"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

// withBudget installs a relay-wide memory budget of max bytes for the test.
func withBudget(t *testing.T, max int64) *memBudget {
	t.Helper()
	b := &memBudget{max: max}
	msgBudget = b
	t.Cleanup(func() { msgBudget = nil })
	return b
}

// waitUsed waits until exactly want bytes of b are reserved.
func waitUsed(t *testing.T, b *memBudget, want int64) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for b.used.Load() != want {
		if time.Now().After(deadline) {
			t.Fatalf("budget in use: %d bytes, want %d", b.used.Load(), want)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// relayBase serves a relay and returns its WebSocket URL, ready for more
// query parameters.
func relayBase(t *testing.T) string {
	t.Helper()
	s := httptest.NewServer(&relayHandler{reg: newRegistry(200)})
	t.Cleanup(s.Close)
	return "ws" + strings.TrimPrefix(s.URL, "http") + "/ws?v=2&"
}

func relayDial(t *testing.T, base, query string) *websocket.Conn {
	t.Helper()
	ws, _, err := websocket.DefaultDialer.Dial(base+query, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ws.Close() })
	return ws
}

// relayRoundTrip sends msg from one peer and expects it to arrive at the other.
func relayRoundTrip(t *testing.T, from, to *websocket.Conn, msg string) {
	t.Helper()
	if err := from.WriteMessage(websocket.TextMessage, []byte(msg)); err != nil {
		t.Fatal(err)
	}
	_ = to.SetReadDeadline(time.Now().Add(5 * time.Second))
	_, got, err := to.ReadMessage()
	if err != nil || string(got) != msg {
		t.Fatalf("relayed %q, got %q (err %v)", msg, got, err)
	}
}

// rawConnPair returns a relay-side conn and the peer that writes to it.
func rawConnPair(t *testing.T) (*conn, *websocket.Conn) {
	t.Helper()
	upgraded := make(chan *websocket.Conn, 1)
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ws, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			t.Error(err)
			return
		}
		upgraded <- ws
	}))
	t.Cleanup(s.Close)
	peer, _, err := websocket.DefaultDialer.Dial("ws"+strings.TrimPrefix(s.URL, "http"), nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = peer.Close() })
	c := newConn(<-upgraded)
	t.Cleanup(func() { _ = c.ws.Close() })
	return c, peer
}

func isTimeout(err error) bool {
	var ne net.Error
	return errors.As(err, &ne) && ne.Timeout()
}

func TestMemBudgetNeverOvercommits(t *testing.T) {
	b := &memBudget{max: 10}
	var overcommitted atomic.Bool
	var wg sync.WaitGroup
	for range 16 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range 2000 {
				if !b.reserve(3) {
					continue
				}
				if b.used.Load() > b.max {
					overcommitted.Store(true)
				}
				b.release(3)
			}
		}()
	}
	wg.Wait()
	if overcommitted.Load() {
		t.Fatal("reservations exceeded the budget")
	}
	if got := b.used.Load(); got != 0 {
		t.Fatalf("%d bytes still reserved after matching releases", got)
	}
	if !b.reserve(10) || b.reserve(1) {
		t.Fatal("budget should fill up to exactly max")
	}

	var unlimited *memBudget
	if !unlimited.reserve(1 << 40) {
		t.Fatal("a nil budget should be unlimited")
	}
	unlimited.release(1 << 40)
}

func TestFrameBufferChargesBudget(t *testing.T) {
	budget := withBudget(t, 100)
	buf := newFrameBuffer(3)
	payload := make([]byte, 9) // 10-byte frames with the type prefix

	for range 4 {
		buf.push(websocket.BinaryMessage, payload)
	}
	if got := budget.used.Load(); got != 30 {
		t.Fatalf("after the item limit dropped a frame: %d bytes charged, want 30", got)
	}

	// A frame the budget cannot hold is dropped and the buffer is unchanged.
	buf.push(websocket.BinaryMessage, make([]byte, 75))
	if got := budget.used.Load(); got != 30 {
		t.Fatalf("after an over-budget push: %d bytes charged, want 30", got)
	}

	// Frames evicted by the byte limit are released.
	buf.maxBytes = 25
	buf.push(websocket.BinaryMessage, payload)
	if got := budget.used.Load(); got != 20 {
		t.Fatalf("after byte-limit eviction: %d bytes charged, want 20", got)
	}

	var sent int
	buf.drain(func(msgType int, data []byte) {
		// Each frame stays charged until it has been sent.
		if got, want := budget.used.Load(), int64(20-10*sent); got != want {
			t.Errorf("while sending frame %d: %d bytes charged, want %d", sent, got, want)
		}
		if msgType != websocket.BinaryMessage || len(data) != len(payload) {
			t.Errorf("frame %d: type %d, %d bytes", sent, msgType, len(data))
		}
		sent++
	})
	if sent != 2 || budget.used.Load() != 0 {
		t.Fatalf("drain sent %d frames, left %d bytes charged", sent, budget.used.Load())
	}

	buf.push(websocket.TextMessage, payload)
	if n := len(buf.flush()); n != 1 || budget.used.Load() != 0 {
		t.Fatalf("flush returned %d frames, left %d bytes charged", n, budget.used.Load())
	}
}

func TestReadMessageChargesBudget(t *testing.T) {
	budget := withBudget(t, 4<<20)
	c, peer := rawConnPair(t)

	for _, size := range []int{0, 1, 511, 512, 513, 70_000, 900_000} {
		want := bytes.Repeat([]byte{byte(size)}, size)
		if err := peer.WriteMessage(websocket.BinaryMessage, want); err != nil {
			t.Fatal(err)
		}
		msgType, got, err := c.readMessage()
		if err != nil || msgType != websocket.BinaryMessage || !bytes.Equal(got, want) {
			t.Fatalf("%d-byte message: type %d, %d bytes, err %v", size, msgType, len(got), err)
		}
		// The whole buffer stays charged until the next read.
		if used := budget.used.Load(); used != int64(cap(got)) || cap(got) > 2*size+512 {
			t.Fatalf("%d-byte message: %d bytes charged for a %d-byte buffer", size, used, cap(got))
		}
	}
	c.releaseRead()
	if used := budget.used.Load(); used != 0 {
		t.Fatalf("after releaseRead: %d bytes charged", used)
	}

	// A message larger than the budget fails part-way and gives back what
	// it had reserved.
	go func() { _ = peer.WriteMessage(websocket.BinaryMessage, make([]byte, 8<<20)) }()
	if _, _, err := c.readMessage(); !errors.Is(err, errMemoryBudget) {
		t.Fatalf("8 MiB message with a 4 MiB budget: err %v, want errMemoryBudget", err)
	}
	if used := budget.used.Load(); used != 0 {
		t.Fatalf("after the failed read: %d bytes charged", used)
	}
}

func TestReadMessageReleasesWhenPeerDropsMidMessage(t *testing.T) {
	budget := withBudget(t, 1<<20)
	c, peer := rawConnPair(t)

	w, err := peer.NextWriter(websocket.BinaryMessage)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := w.Write(make([]byte, 100_000)); err != nil { // sent as non-final frames
		t.Fatal(err)
	}
	_ = peer.UnderlyingConn().Close() // never finish the message

	if _, _, err := c.readMessage(); err == nil {
		t.Fatal("reading a truncated message succeeded")
	}
	if used := budget.used.Load(); used != 0 {
		t.Fatalf("after a truncated message: %d bytes charged", used)
	}
}

func TestReadMessageOverBudgetClosesWith1013(t *testing.T) {
	budget := withBudget(t, 1<<20)
	c, peer := rawConnPair(t)
	if !budget.reserve(1 << 20) {
		t.Fatal("could not fill the budget")
	}
	defer budget.release(1 << 20)

	if err := peer.WriteMessage(websocket.TextMessage, []byte("hi")); err != nil {
		t.Fatal(err)
	}
	if _, _, err := c.readMessage(); !errors.Is(err, errMemoryBudget) {
		t.Fatalf("read with a full budget: err %v, want errMemoryBudget", err)
	}
	_ = peer.SetReadDeadline(time.Now().Add(5 * time.Second))
	if _, _, err := peer.ReadMessage(); !websocket.IsCloseError(err, websocket.CloseTryAgainLater) {
		t.Fatalf("peer read %v, want close 1013", err)
	}
}

// expectFrame reads one message of size bytes from ws.
func expectFrame(t *testing.T, ws *websocket.Conn, size int) {
	t.Helper()
	_ = ws.SetReadDeadline(time.Now().Add(5 * time.Second))
	_, got, err := ws.ReadMessage()
	if err != nil || len(got) != size {
		t.Fatalf("got a %d-byte message (err %v), want %d bytes", len(got), err, size)
	}
}

func TestRelayBoundsMemoryEndToEnd(t *testing.T) {
	budget := withBudget(t, 8<<20)
	base := relayBase(t)

	// With no daemon attached, a's 3 MiB message is buffered. Reading and
	// copying it briefly takes about twice its size; afterwards only the
	// buffered frame stays charged.
	a := relayDial(t, base, "serverId=a&role=client&connectionId=c")
	if err := a.WriteMessage(websocket.BinaryMessage, make([]byte, 3<<20)); err != nil {
		t.Fatal(err)
	}
	waitUsed(t, budget, 3<<20+1)

	// b's 6 MiB message does not fit in the 5 MiB left, so it cannot be
	// read: b is disconnected and its partial read is released.
	b := relayDial(t, base, "serverId=b&role=client&connectionId=c")
	go func() { _ = b.WriteMessage(websocket.BinaryMessage, make([]byte, 6<<20)) }()
	_ = b.SetReadDeadline(time.Now().Add(5 * time.Second))
	if _, _, err := b.ReadMessage(); err == nil || isTimeout(err) {
		t.Fatalf("over-budget client: read err %v, want a disconnect", err)
	}
	waitUsed(t, budget, 3<<20+1)

	// Small messages still flow between a daemon and its client.
	daemon := relayDial(t, base, "serverId=s&role=server&connectionId=x")
	client := relayDial(t, base, "serverId=s&role=client&connectionId=x")
	relayRoundTrip(t, client, daemon, "hello daemon")
	relayRoundTrip(t, daemon, client, "hello client")

	// When a's daemon attaches, it gets the buffered frame; then nothing
	// stays charged.
	expectFrame(t, relayDial(t, base, "serverId=a&role=server&connectionId=c"), 3<<20)
	waitUsed(t, budget, 0)
}

func TestHandoverKeepsFramesChargedUntilSent(t *testing.T) {
	budget := withBudget(t, 64<<20)
	base := relayBase(t)
	const size = 24 << 20 // well past what loopback socket buffers absorb

	client := relayDial(t, base, "serverId=s&role=client&connectionId=c")
	if err := client.WriteMessage(websocket.BinaryMessage, make([]byte, size)); err != nil {
		t.Fatal(err)
	}
	waitUsed(t, budget, size+1)

	// A daemon that does not read stalls the handover send; the frame must
	// stay charged meanwhile rather than slip out of the budget.
	daemon := relayDial(t, base, "serverId=s&role=server&connectionId=c")
	time.Sleep(300 * time.Millisecond)
	if used := budget.used.Load(); used != size+1 {
		t.Fatalf("while the handover send is stalled: %d bytes charged, want %d", used, size+1)
	}

	expectFrame(t, daemon, size)
	waitUsed(t, budget, 0)
}

func TestRelayReleasesBudgetUnderChurn(t *testing.T) {
	budget := withBudget(t, 64<<20)
	base := relayBase(t)

	var wg sync.WaitGroup
	for w := range 16 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			rng := rand.New(rand.NewPCG(uint64(w), 0))
			for range 25 {
				server, conn := rng.IntN(3), rng.IntN(3)
				query := fmt.Sprintf("serverId=s%d&role=client&connectionId=c%d", server, conn)
				switch rng.IntN(4) {
				case 0:
					query = fmt.Sprintf("serverId=s%d&role=server", server)
				case 1:
					query = fmt.Sprintf("serverId=s%d&role=server&connectionId=c%d", server, conn)
				}
				ws, _, err := websocket.DefaultDialer.Dial(base+query, nil)
				if err != nil {
					t.Error(err)
					return
				}
				// Bound writes and reads: peers that stop reading must not
				// stall the test.
				_ = ws.SetWriteDeadline(time.Now().Add(200 * time.Millisecond))
				for range rng.IntN(5) {
					if ws.WriteMessage(websocket.BinaryMessage, make([]byte, rng.IntN(300_000))) != nil {
						break
					}
				}
				if rng.IntN(2) == 0 {
					_ = ws.SetReadDeadline(time.Now().Add(20 * time.Millisecond))
					for {
						if _, _, err := ws.ReadMessage(); err != nil {
							break
						}
					}
				}
				_ = ws.Close()
			}
		}()
	}
	wg.Wait()
	waitUsed(t, budget, 0)
}
