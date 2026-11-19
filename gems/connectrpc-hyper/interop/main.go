// Disposable local peer for the connectrpc-hyper specs and interop check.
//
// It starts grpc-go (h2c, TLS, mTLS, keepalive, idle, max-age and a
// restartable listener), connect-go (HTTP/1.1 + h2c, TLS, mTLS and an
// HTTP/1.1-only TLS listener), a programmable raw HTTP/2 server and an HTTP
// proxy (CONNECT and absolute-form forwarding) on 127.0.0.1, prints one JSON line of origins, then one "EVENT ..." line per
// observable server-side event. stdin carries control commands, one per line;
// the process exits when stdin closes. Nothing listens beyond loopback.
package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"strings"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"
)

var outMu sync.Mutex

func emit(format string, args ...any) {
	outMu.Lock()
	defer outMu.Unlock()
	fmt.Printf("EVENT "+format+"\n", args...)
}

func must(err error) {
	if err != nil {
		panic(err)
	}
}

// ---------- gates released by the "release <tag>" command ----------

var gates = struct {
	sync.Mutex
	channels map[string]chan struct{}
}{channels: map[string]chan struct{}{}}

func gate(tag string) chan struct{} {
	gates.Lock()
	defer gates.Unlock()
	channel, ok := gates.channels[tag]
	if !ok {
		channel = make(chan struct{})
		gates.channels[tag] = channel
	}
	return channel
}

func release(tag string) {
	channel := gate(tag)
	gates.Lock()
	defer gates.Unlock()
	select {
	case <-channel:
	default:
		close(channel)
	}
}

// ---------- connection events ----------

type eventListener struct {
	net.Listener
	tag   string
	reads bool

	mu    sync.Mutex
	conns map[*eventConn]struct{}
}

type eventConn struct {
	net.Conn
	listener *eventListener
	once     sync.Once
}

func newEventListener(listener net.Listener, tag string, reads bool) *eventListener {
	return &eventListener{Listener: listener, tag: tag, reads: reads, conns: map[*eventConn]struct{}{}}
}

func (l *eventListener) Accept() (net.Conn, error) {
	conn, err := l.Listener.Accept()
	if err != nil {
		return nil, err
	}
	emit("%s accept %s", l.tag, conn.RemoteAddr())
	wrapped := &eventConn{Conn: conn, listener: l}
	l.mu.Lock()
	l.conns[wrapped] = struct{}{}
	l.mu.Unlock()
	return wrapped, nil
}

// kill closes every accepted connection with a TCP reset, as a crashed
// server would, without GOAWAY.
func (l *eventListener) kill() {
	l.mu.Lock()
	conns := make([]*eventConn, 0, len(l.conns))
	for conn := range l.conns {
		conns = append(conns, conn)
	}
	l.mu.Unlock()
	for _, conn := range conns {
		if tcp, ok := conn.Conn.(*net.TCPConn); ok {
			_ = tcp.SetLinger(0)
		}
		_ = conn.Close()
	}
}

// Read reports every inbound read on listeners that ask for it: on an idle
// connection that is the client's PING ACK, GOAWAY or close.
func (c *eventConn) Read(b []byte) (int, error) {
	n, err := c.Conn.Read(b)
	if n > 0 && c.listener.reads {
		emit("%s read %d", c.listener.tag, n)
	}
	return n, err
}

func (c *eventConn) Close() error {
	c.once.Do(func() {
		emit("%s conn-closed %s", c.listener.tag, c.RemoteAddr())
		c.listener.mu.Lock()
		delete(c.listener.conns, c)
		c.listener.mu.Unlock()
	})
	return c.Conn.Close()
}

func listen(address string) net.Listener {
	listener, err := net.Listen("tcp", address)
	must(err)
	return listener
}

// ---------- a grpc-go listener that can be killed and restarted ----------

type restartable struct {
	mu       sync.Mutex
	address  string
	server   *grpc.Server
	listener *eventListener
}

func (r *restartable) start() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.listener = newEventListener(listen(r.address), "restart", false)
	r.server = grpc.NewServer()
	registerGRPC(r.server, "restart")
	server, listener := r.server, r.listener
	go func() { _ = server.Serve(listener) }()
}

func (r *restartable) kill() {
	r.mu.Lock()
	defer r.mu.Unlock()
	_ = r.listener.Close()
	r.listener.kill()
	r.server.Stop()
}

func (r *restartable) graceful() {
	r.mu.Lock()
	server := r.server
	r.mu.Unlock()
	server.GracefulStop()
}

func control(restart *restartable) {
	scanner := bufio.NewScanner(os.Stdin)
	for scanner.Scan() {
		fields := strings.Fields(scanner.Text())
		if len(fields) == 0 {
			continue
		}
		switch fields[0] {
		case "release":
			release(fields[1])
		case "raw-goaway":
			rawGoaway(fields[1])
		case "kill":
			restart.kill()
			restart.start()
		case "graceful":
			restart.graceful()
			restart.start()
		}
		emit("control done %s", strings.Join(fields, " "))
	}
}

func main() {
	if len(os.Args) != 2 {
		panic("usage: peer CERT_DIR")
	}
	server, mtls, h1only := certificates(os.Args[1])
	restart := &restartable{address: "127.0.0.1:0"}
	restart.start()
	restart.address = restart.listener.Addr().String()
	origins := map[string]string{
		"grpc_h2c":       serveGRPC("grpc", nil),
		"grpc_tls":       serveGRPC("grpc", server.Clone()),
		"grpc_mtls":      serveGRPC("grpc", mtls.Clone()),
		"connect_h2c":    serveConnect("connect", nil),
		"connect_tls":    serveConnect("connect", server.Clone()),
		"connect_mtls":   serveConnect("connect", mtls.Clone()),
		"connect_tls_h1": serveConnect("connecth1", h1only),
		"raw":            serveRaw("raw", rawOptions{ack: true}),
		"raw_noack":      serveRaw("rawnoack", rawOptions{ack: false}),
		"raw_mcs1":       serveRaw("rawmcs1", rawOptions{ack: true, maxStreams: 1}),
		"grpc_restart":   "http://" + restart.address,
		"proxy":          serveProxy(),
		// Server PINGs every 100 ms; the connection closes when an ACK does not
		// arrive within 100 ms.
		"grpc_keepalive": serveGRPCWith("ka", true, grpc.KeepaliveParams(keepalive.ServerParameters{
			Time: 100 * time.Millisecond, Timeout: 100 * time.Millisecond})),
		// GOAWAY after 300 ms without active streams.
		"grpc_idle": serveGRPCWith("idle", false, grpc.KeepaliveParams(keepalive.ServerParameters{
			MaxConnectionIdle: 300 * time.Millisecond})),
		// Static 64 KiB flow-control windows: grpc-go's BDP estimator never
		// grows them, so a writer the handler does not read stays blocked.
		"grpc_static": serveGRPCWith("static", false, grpc.StaticStreamWindowSize(65535)),
		// GOAWAY 300 ms after accept; active streams then get 30 s to finish.
		"grpc_age": serveGRPCWith("age", false, grpc.KeepaliveParams(keepalive.ServerParameters{
			MaxConnectionAge: 300 * time.Millisecond, MaxConnectionAgeGrace: 30 * time.Second})),
	}
	line, err := json.Marshal(origins)
	must(err)
	outMu.Lock()
	fmt.Println(string(line))
	outMu.Unlock()
	control(restart)
}
