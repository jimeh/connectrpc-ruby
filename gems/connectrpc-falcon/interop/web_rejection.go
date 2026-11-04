package main

import (
	"bufio"
	"bytes"
	"context"
	"crypto/tls"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/dynamicpb"
)

// Gate the return of the write containing the COMPLETE header separator. Buffer
// split headers and withhold any coalesced entity bytes until dispatch waits.
// Embedding only net.Conn deliberately exposes no io.ReaderFrom fast path.
type headerGate struct {
	net.Conn
	headers  []byte
	sent     bool
	waiting  atomic.Bool
	released atomic.Bool
	early    atomic.Bool
	once     sync.Once
	release  chan struct{}
	state    tls.ConnectionState
}

func (g *headerGate) ConnectionState() tls.ConnectionState { return g.state }
func (g *headerGate) resume(early bool) {
	if early && !g.released.Load() {
		g.early.Store(true)
	}
	g.once.Do(func() { g.released.Store(true); close(g.release) })
}
func (g *headerGate) Write(p []byte) (int, error) {
	if g.sent {
		return g.Conn.Write(p)
	}
	g.headers = append(g.headers, p...)
	end := bytes.Index(g.headers, []byte("\r\n\r\n"))
	if end < 0 {
		return len(p), nil
	}
	g.sent = true
	g.waiting.Store(true)
	if _, err := io.Copy(g.Conn, bytes.NewReader(g.headers[:end+4])); err != nil {
		return 0, err
	}
	<-g.release
	if _, err := io.Copy(g.Conn, bytes.NewReader(g.headers[end+4:])); err != nil {
		return 0, err
	}
	g.headers = nil
	return len(p), nil
}
func (g *headerGate) Read(p []byte) (int, error) {
	n, err := g.Conn.Read(p)
	if n > 0 && g.waiting.Load() {
		g.resume(true)
	}
	return n, err
}

type capturedWebBody struct {
	io.ReadCloser
	bytes bytes.Buffer
	eof   bool
}

func (b *capturedWebBody) Read(p []byte) (int, error) {
	n, err := b.ReadCloser.Read(p)
	b.bytes.Write(p[:n])
	if err == io.EOF {
		b.eof = true
	}
	return n, err
}

type webHeadTransport struct {
	transport *http.Transport
	gate      **headerGate
	body      *capturedWebBody
}

func (t *webHeadTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	response, err := t.transport.RoundTrip(r)
	(*t.gate).resume(true) // Do completion must also release, and expose baseline red.
	if err != nil {
		return nil, err
	}
	check(response.StatusCode == 200 && response.ProtoMajor == 1, "Web HTTP 200 over H1")
	t.body = &capturedWebBody{ReadCloser: response.Body}
	response.Body = t.body
	return response, nil
}

func webRejection(service protoreflect.ServiceDescriptor, origin, directory string) {
	var gate *headerGate
	protocols := new(http.Protocols)
	protocols.SetHTTP1(true)
	transport := &http.Transport{Protocols: protocols, Proxy: nil, DisableCompression: true}
	dial := func(ctx context.Context, network, address string) (net.Conn, error) {
		conn, err := (&net.Dialer{}).DialContext(ctx, network, address)
		if err != nil {
			return nil, err
		}
		gate = &headerGate{Conn: conn, release: make(chan struct{})}
		if strings.HasPrefix(origin, "https:") {
			config := tlsConfig(directory, false, false)
			config.ServerName = "localhost"
			config.NextProtos = []string{"http/1.1"}
			ssl := tls.Client(conn, config)
			if err := ssl.HandshakeContext(ctx); err != nil {
				conn.Close()
				return nil, err
			}
			gate.Conn = ssl // The gate is ABOVE TLS, never on encrypted record writes.
			gate.state = ssl.ConnectionState()
			check(gate.state.NegotiatedProtocol == "http/1.1", "Web header gate ALPN")
		}
		go func() {
			scanner := bufio.NewScanner(os.Stdin)
			check(scanner.Scan() && scanner.Text() == "dispatch_wait", "missing dispatch control event")
			gate.resume(false)
		}()
		return gate, nil
	}
	transport.DialContext = dial
	transport.DialTLSContext = dial
	defer transport.CloseIdleConnections()
	observed := &webHeadTransport{transport: transport, gate: &gate}
	method := service.Methods().ByName("Echo")
	peer := connect.NewClient[dynamicpb.Message, dynamicpb.Message](
		&http.Client{Transport: observed, Timeout: 5 * time.Second}, origin+"/web.Rejection/Watch",
		connect.WithGRPCWeb(), connect.WithResponseInitializer(initializer), connect.WithSchema(method))
	message := dynamicpb.NewMessage(method.Input())
	message.Set(method.Input().Fields().ByName("name"), protoreflect.ValueOfString(strings.Repeat("x", 200000)))
	stream, err := peer.CallServerStream(context.Background(), connect.NewRequest(message))
	must(err)
	check(!stream.Receive(), "rejected Web stream unexpectedly produced a message")
	check(connect.CodeOf(stream.Err()) == connect.CodeResourceExhausted, "Web resource_exhausted")
	_, err = io.Copy(io.Discard, observed.body)
	must(err)
	must(stream.Close())
	terminal := observed.body.bytes.Bytes()
	check(observed.body.eof, "Web actual HTTP completion absent")
	check(len(terminal) >= 5 && terminal[0] == 0x80 && int(binary.BigEndian.Uint32(terminal[1:5])) == len(terminal)-5,
		"Web complete single terminal frame")
	check(bytes.Contains(terminal[5:], []byte("grpc-status: 8\r\n")), "Web terminal resource_exhausted")
	fmt.Printf("Web H1 %s resource_exhausted, complete terminal and HTTP EOF; early_head=%t\n", origin, gate.early.Load())
	check(!gate.early.Load(), "early_head=true: response head preceded dispatch completion wait")
}
