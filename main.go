// Paseo relay server — zero-knowledge WebSocket relay.
//
// The relay bridges Paseo daemon and mobile app connections without being
// able to read the traffic. All messages are E2E encrypted by the clients;
// the relay only forwards bytes.
//
// Protocol (v2):
//
//	GET /ws?serverId=<id>&role=server&v=2                          — daemon control socket
//	GET /ws?serverId=<id>&role=server&connectionId=<c>&v=2         — daemon data socket
//	GET /ws?serverId=<id>&role=client[&connectionId=<c>]&v=2       — client socket
//
// GET /health — returns {"status":"ok","version":"<version>"}
package main

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/gorilla/websocket"
)

// version is set at build time via -ldflags "-X main.version=v1.2.3".
var version = "dev"

// ---- WebSocket upgrader ----

// maxMessageBytes is the maximum size of a single WebSocket message.
const maxMessageBytes = 32 * 1024 * 1024 // 32 MiB

// maxPendingBytes is the maximum total bytes buffered per pending frameBuffer.
const maxPendingBytes = maxMessageBytes + 1 // message plus 1-byte type prefix

var upgrader = websocket.Upgrader{
	HandshakeTimeout: 10 * time.Second,
	CheckOrigin:      func(r *http.Request) bool { return true },
}

// ---- Thread-safe WebSocket connection ----

// conn wraps a gorilla WebSocket with a write mutex.
// gorilla allows one concurrent reader and one concurrent writer;
// reads are always done from a single goroutine per conn, so only
// writes need to be serialized.
type conn struct {
	mu sync.Mutex
	ws *websocket.Conn
}

func newConn(ws *websocket.Conn) *conn { return &conn{ws: ws} }

func (c *conn) send(msgType int, data []byte) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.ws.WriteMessage(msgType, data)
}

func (c *conn) close(code int, reason string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	msg := websocket.FormatCloseMessage(code, reason)
	_ = c.ws.WriteMessage(websocket.CloseMessage, msg)
	_ = c.ws.Close()
}

// ---- Frame buffer ----

type frameBuffer struct {
	mu         sync.Mutex
	frames     [][]byte
	maxItems   int
	maxBytes   int
	totalBytes int
}

func newFrameBuffer(maxItems int) *frameBuffer {
	return &frameBuffer{maxItems: maxItems, maxBytes: maxPendingBytes}
}

// push stores a frame. Each frame is prefixed with a 1-byte msgType so it can
// be replayed with the correct WebSocket message type.
func (b *frameBuffer) push(msgType int, data []byte) {
	frame := make([]byte, 1+len(data))
	frame[0] = byte(msgType)
	copy(frame[1:], data)

	b.mu.Lock()
	defer b.mu.Unlock()

	// Drop the new frame if it alone exceeds the byte limit.
	if len(frame) > b.maxBytes {
		return
	}

	// Evict oldest frames until we have room for the new one.
	for b.totalBytes+len(frame) > b.maxBytes && len(b.frames) > 0 {
		b.totalBytes -= len(b.frames[0])
		b.frames = b.frames[1:]
	}

	b.frames = append(b.frames, frame)
	b.totalBytes += len(frame)

	// Also enforce the item count limit by dropping oldest.
	if len(b.frames) > b.maxItems {
		dropped := b.frames[:len(b.frames)-b.maxItems]
		for _, f := range dropped {
			b.totalBytes -= len(f)
		}
		b.frames = b.frames[len(b.frames)-b.maxItems:]
	}
}

// flush returns all buffered frames and clears the buffer.
func (b *frameBuffer) flush() [][]byte {
	b.mu.Lock()
	defer b.mu.Unlock()
	out := b.frames
	b.frames = nil
	b.totalBytes = 0
	return out
}

// isEmpty reports whether the buffer holds no frames.
func (b *frameBuffer) isEmpty() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	return len(b.frames) == 0
}

// ---- Pipe ----

// pipe holds per-connectionId state. The forwarding hot-path only needs
// pipe.mu.RLock(), never the session-level lock.
type pipe struct {
	mu         sync.RWMutex
	serverData *conn
	clients    []*conn
	pending    *frameBuffer
}

// sendOrBuffer re-checks serverData under the write lock before buffering.
// The caller already observed a nil snapshot; without the re-check a concurrent
// handover can drain pending and leave this frame stranded.
func (p *pipe) sendOrBuffer(msgType int, msg []byte) error {
	p.mu.Lock()
	srv := p.serverData
	if srv != nil {
		p.mu.Unlock()
		return srv.send(msgType, msg)
	}
	p.pending.push(msgType, msg)
	p.mu.Unlock()
	return nil
}

// isEmpty reports whether the pipe has no active connections and no buffered
// frames — safe to remove from the pipes map.
func (p *pipe) isEmpty() bool {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.serverData == nil && len(p.clients) == 0 && p.pending.isEmpty()
}

// ---- Session ----

type session struct {
	// ctlMu protects only the control connection.
	ctlMu sync.RWMutex
	control *conn

	// pipeMu protects only the pipes map (held briefly for map ops).
	pipeMu sync.Mutex
	pipes  map[string]*pipe

	maxBufferFrames int
}

func newSession(maxBufferFrames int) *session {
	return &session{
		pipes:           make(map[string]*pipe),
		maxBufferFrames: maxBufferFrames,
	}
}

// getOrCreatePipe returns the pipe for connectionId, creating it if needed.
func (s *session) getOrCreatePipe(connectionId string) *pipe {
	s.pipeMu.Lock()
	defer s.pipeMu.Unlock()
	p, ok := s.pipes[connectionId]
	if !ok {
		p = &pipe{pending: newFrameBuffer(s.maxBufferFrames)}
		s.pipes[connectionId] = p
	}
	return p
}

// getPipe returns the pipe for connectionId, or nil if it doesn't exist.
func (s *session) getPipe(connectionId string) *pipe {
	s.pipeMu.Lock()
	defer s.pipeMu.Unlock()
	return s.pipes[connectionId]
}

// removePipeIfEmpty removes the pipe for connectionId from the map if it is
// empty. Must be called after releasing pipe.mu.
func (s *session) removePipeIfEmpty(connectionId string, p *pipe) {
	if !p.isEmpty() {
		return
	}
	s.pipeMu.Lock()
	defer s.pipeMu.Unlock()
	// Only delete if the map entry still points to this exact pipe.
	if s.pipes[connectionId] == p {
		delete(s.pipes, connectionId)
	}
}

func (s *session) isEmpty() bool {
	s.ctlMu.RLock()
	hasControl := s.control != nil
	s.ctlMu.RUnlock()
	if hasControl {
		return false
	}
	s.pipeMu.Lock()
	nPipes := len(s.pipes)
	s.pipeMu.Unlock()
	return nPipes == 0
}

func (s *session) connectedConnectionIds() []string {
	s.pipeMu.Lock()
	ids := make([]string, 0, len(s.pipes))
	for id, p := range s.pipes {
		p.mu.RLock()
		hasClients := len(p.clients) > 0
		p.mu.RUnlock()
		if hasClients {
			ids = append(ids, id)
		}
	}
	s.pipeMu.Unlock()
	return ids
}

func (s *session) notifyControl(msg any) {
	s.ctlMu.RLock()
	ctl := s.control
	s.ctlMu.RUnlock()
	if ctl == nil {
		return
	}
	data, err := json.Marshal(msg)
	if err != nil {
		return
	}
	if err := ctl.send(websocket.TextMessage, data); err != nil {
		ctl.close(1011, "Control send failed")
	}
}

// nudgeOrResetControl watches whether the daemon opens a data socket for
// connectionId after a client connects. If not, it nudges the control socket
// with a sync message; if still no reaction, force-closes the control socket
// so the daemon reconnects.
func (s *session) nudgeOrResetControl(connectionId string) {
	time.AfterFunc(10*time.Second, func() {
		p := s.getPipe(connectionId)
		if p == nil {
			return
		}
		p.mu.RLock()
		hasClient := len(p.clients) > 0
		hasData := p.serverData != nil
		p.mu.RUnlock()

		if !hasClient || hasData {
			return
		}
		s.notifyControl(map[string]any{
			"type":          "sync",
			"connectionIds": s.connectedConnectionIds(),
		})

		// Capture control pointer now for the deferred check.
		s.ctlMu.RLock()
		ctl := s.control
		s.ctlMu.RUnlock()

		time.AfterFunc(5*time.Second, func() {
			p2 := s.getPipe(connectionId)
			if p2 == nil {
				return
			}
			p2.mu.RLock()
			hasClient2 := len(p2.clients) > 0
			hasData2 := p2.serverData != nil
			p2.mu.RUnlock()

			if !hasClient2 || hasData2 {
				return
			}

			// Only close if control hasn't been replaced since we captured it.
			s.ctlMu.Lock()
			if s.control == ctl && s.control != nil {
				s.control = nil
				s.ctlMu.Unlock()
				ctl.close(1011, "Control unresponsive")
			} else {
				s.ctlMu.Unlock()
			}
		})
	})
}

// ---- Session registry ----

// maxSessions is the maximum number of concurrent sessions allowed.
const maxSessions = 10_000

type registry struct {
	mu              sync.Mutex
	sessions        map[string]*session
	maxBufferFrames int
}

func newRegistry(maxBufferFrames int) *registry {
	return &registry{
		sessions:        make(map[string]*session),
		maxBufferFrames: maxBufferFrames,
	}
}

// get returns the session for serverId, creating it if needed.
// Returns (session, true) on success, or (nil, false) if the session limit is reached.
func (r *registry) get(serverId string) (*session, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	s, ok := r.sessions[serverId]
	if !ok {
		if len(r.sessions) >= maxSessions {
			return nil, false
		}
		s = newSession(r.maxBufferFrames)
		r.sessions[serverId] = s
	}
	return s, true
}

// remove deletes the session for serverId if it is still the same pointer and
// is now empty. Called at handler exit for active cleanup.
func (r *registry) remove(serverId string, sess *session) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.sessions[serverId] == sess && sess.isEmpty() {
		delete(r.sessions, serverId)
	}
}

// evict removes sessions that have no active connections.
func (r *registry) evict() {
	r.mu.Lock()
	defer r.mu.Unlock()
	for id, s := range r.sessions {
		if s.isEmpty() {
			delete(r.sessions, id)
		}
	}
}

// startEvictionLoop runs a background goroutine that periodically evicts
// empty sessions to prevent unbounded memory growth. This is a safety-net
// backstop; active cleanup happens in each handler's defer block.
func (r *registry) startEvictionLoop(ctx context.Context, interval time.Duration) {
	go func() {
		t := time.NewTicker(interval)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				r.evict()
			}
		}
	}()
}

// ---- HTTP handler ----

type relayHandler struct {
	reg *registry
}

func (h *relayHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	switch r.URL.Path {
	case "/health":
		w.Header().Set("Content-Type", "application/json")
		data, _ := json.Marshal(map[string]string{"status": "ok", "version": version})
		_, _ = w.Write(data)
	case "/ws":
		h.handleWS(w, r)
	default:
		http.NotFound(w, r)
	}
}

func (h *relayHandler) handleWS(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	serverId := q.Get("serverId")
	role := q.Get("role")
	connectionId := q.Get("connectionId")
	v := q.Get("v")

	if serverId == "" {
		http.Error(w, "Missing serverId", http.StatusBadRequest)
		return
	}
	if role != "server" && role != "client" {
		http.Error(w, "Invalid role (expected server or client)", http.StatusBadRequest)
		return
	}
	if v != "2" {
		http.Error(w, "Only v=2 is supported", http.StatusBadRequest)
		return
	}

	ws, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		slog.Error("websocket upgrade failed", "err", err)
		return
	}
	ws.SetReadLimit(maxMessageBytes)

	sess, ok := h.reg.get(serverId)
	if !ok {
		ws.WriteMessage(websocket.CloseMessage,
			websocket.FormatCloseMessage(1008, "Too many sessions"))
		ws.Close()
		slog.Warn("session limit reached, rejected connection", "serverId", serverId)
		return
	}
	c := newConn(ws)

	switch role {
	case "server":
		if connectionId == "" {
			h.handleServerControl(sess, c, serverId)
		} else {
			h.handleServerData(sess, c, serverId, connectionId)
		}
	case "client":
		if connectionId == "" {
			connectionId = "conn_" + randomHex(8)
		}
		h.handleClient(sess, c, serverId, connectionId)
	}
}

// handleServerControl manages the daemon's control socket (one per session).
func (h *relayHandler) handleServerControl(sess *session, c *conn, serverId string) {
	slog.Info("server-control connected", "serverId", serverId)

	sess.ctlMu.Lock()
	old := sess.control
	sess.control = c
	sess.ctlMu.Unlock()
	if old != nil {
		old.close(1008, "Replaced by new connection")
	}

	// Send current connection list so the daemon can attach existing clients.
	ids := sess.connectedConnectionIds()
	data, _ := json.Marshal(map[string]any{"type": "sync", "connectionIds": ids})
	_ = c.send(websocket.TextMessage, data)

	defer func() {
		sess.ctlMu.Lock()
		if sess.control == c {
			sess.control = nil
		}
		sess.ctlMu.Unlock()
		slog.Info("server-control disconnected", "serverId", serverId)
		h.reg.remove(serverId, sess)
	}()

	for {
		msgType, msg, err := c.ws.ReadMessage()
		if err != nil {
			break
		}
		// Respond to ping keepalives on the control channel.
		if msgType == websocket.TextMessage {
			var parsed map[string]any
			if json.Unmarshal(msg, &parsed) == nil {
				if parsed["type"] == "ping" {
					resp, _ := json.Marshal(map[string]any{"type": "pong", "ts": time.Now().UnixMilli()})
					_ = c.send(websocket.TextMessage, resp)
					continue
				}
			}
		}
		// Control channel carries no data frames; ignore anything else.
	}
}

// handleServerData manages a daemon data socket for a specific connectionId.
func (h *relayHandler) handleServerData(sess *session, c *conn, serverId, connectionId string) {
	slog.Info("server-data connected", "serverId", serverId, "connectionId", connectionId)

	p := sess.getOrCreatePipe(connectionId)

	// Atomically set serverData, swap out pending with a fresh buffer, and
	// drain the captured frames under the write lock. Holding the lock
	// during the drain keeps handleClient from sending a newer frame before
	// the pre-buffered ones. Swapping pending means a late push cannot land
	// in a buffer that will never be drained again.
	p.mu.Lock()
	old := p.serverData
	p.serverData = c
	oldBuf := p.pending
	p.pending = newFrameBuffer(sess.maxBufferFrames)
	flushed := oldBuf.flush()
	for _, frame := range flushed {
		if len(frame) == 0 {
			continue
		}
		_ = c.send(int(frame[0]), frame[1:])
	}
	p.mu.Unlock()

	if old != nil {
		old.close(1008, "Replaced by new connection")
	}

	defer func() {
		p.mu.Lock()
		if p.serverData == c {
			p.serverData = nil
		}
		clients := append([]*conn(nil), p.clients...)
		p.mu.Unlock()

		// Force clients to reconnect and re-handshake when the daemon drops.
		for _, cl := range clients {
			cl.close(1012, "Server disconnected")
		}
		slog.Info("server-data disconnected", "serverId", serverId, "connectionId", connectionId)
		sess.removePipeIfEmpty(connectionId, p)
		h.reg.remove(serverId, sess)
	}()

	for {
		msgType, msg, err := c.ws.ReadMessage()
		if err != nil {
			break
		}
		// Hot path: only pipe.mu.RLock() — no session-level lock.
		p.mu.RLock()
		targets := append([]*conn(nil), p.clients...)
		p.mu.RUnlock()
		for _, cl := range targets {
			if err := cl.send(msgType, msg); err != nil {
				slog.Error("forward server->client failed", "connectionId", connectionId, "err", err)
			}
		}
	}
}

// handleClient manages an app/client socket (multiple allowed per connectionId).
func (h *relayHandler) handleClient(sess *session, c *conn, serverId, connectionId string) {
	slog.Info("client connected", "serverId", serverId, "connectionId", connectionId)

	p := sess.getOrCreatePipe(connectionId)

	p.mu.Lock()
	p.clients = append(p.clients, c)
	p.mu.Unlock()

	sess.notifyControl(map[string]any{"type": "connected", "connectionId": connectionId})
	sess.nudgeOrResetControl(connectionId)

	defer func() {
		p.mu.Lock()
		list := p.clients
		newList := make([]*conn, 0, len(list))
		for _, cl := range list {
			if cl != c {
				newList = append(newList, cl)
			}
		}
		isLast := len(newList) == 0
		var srv *conn
		if isLast {
			p.clients = nil
			// Clear the pending buffer too; we're the last client.
			p.pending.flush()
			srv = p.serverData
		} else {
			p.clients = newList
		}
		p.mu.Unlock()

		if isLast {
			if srv != nil {
				srv.close(1001, "Client disconnected")
			}
			sess.notifyControl(map[string]any{"type": "disconnected", "connectionId": connectionId})
			sess.removePipeIfEmpty(connectionId, p)
		}
		slog.Info("client disconnected", "serverId", serverId, "connectionId", connectionId)
		h.reg.remove(serverId, sess)
	}()

	for {
		msgType, msg, err := c.ws.ReadMessage()
		if err != nil {
			break
		}
		// Hot path: only pipe.mu.RLock() — no session-level lock.
		p.mu.RLock()
		srv := p.serverData
		p.mu.RUnlock()

		if srv == nil {
			if err := p.sendOrBuffer(msgType, msg); err != nil {
				slog.Error("forward client->server failed", "connectionId", connectionId, "err", err)
			}
			continue
		}
		if err := srv.send(msgType, msg); err != nil {
			slog.Error("forward client->server failed", "connectionId", connectionId, "err", err)
		}
	}
}

// ---- Helpers ----

func randomHex(n int) string {
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		panic("crypto/rand unavailable: " + err.Error())
	}
	const hx = "0123456789abcdef"
	out := make([]byte, n*2)
	for i, v := range b {
		out[i*2] = hx[v>>4]
		out[i*2+1] = hx[v&0xf]
	}
	return string(out)
}

func envOrDefault(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

// ---- Main ----

func main() {
	addr := flag.String("addr", envOrDefault("RELAY_ADDR", ":8411"), "listen address")
	maxBuf := flag.Int("max-buffer-frames", 200, "max frames buffered per connection while daemon is connecting")
	logFormat := flag.String("log-format", envOrDefault("LOG_FORMAT", "text"), "log format: text or json")
	printVersion := flag.Bool("version", false, "print version and exit")
	flag.Parse()

	if *printVersion {
		fmt.Println(version)
		os.Exit(0)
	}

	// Configure structured logging.
	var handler slog.Handler
	opts := &slog.HandlerOptions{Level: slog.LevelInfo}
	if *logFormat == "json" {
		handler = slog.NewJSONHandler(os.Stdout, opts)
	} else {
		handler = slog.NewTextHandler(os.Stdout, opts)
	}
	slog.SetDefault(slog.New(handler))

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	reg := newRegistry(*maxBuf)
	reg.startEvictionLoop(ctx, 5*time.Minute)

	srv := &http.Server{
		Addr:    *addr,
		Handler: &relayHandler{reg: reg},
	}

	go func() {
		slog.Info("paseo relay starting", "addr", *addr, "version", version)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			slog.Error("server error", "err", err)
			os.Exit(1)
		}
	}()

	<-ctx.Done()
	slog.Info("shutting down...")

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		slog.Error("shutdown error", "err", err)
	}
	slog.Info("stopped")
}
