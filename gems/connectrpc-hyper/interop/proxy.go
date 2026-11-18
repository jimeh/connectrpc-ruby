package main

import (
	"bufio"
	"context"
	"io"
	"net"
	"net/http"
	"strings"
)

// A disposable HTTP proxy on 127.0.0.1: CONNECT tunnels and absolute-form
// forwarding. Names under .test stand for this machine, so a client that
// reaches a .test origin proves the proxy did the resolving; nothing leaves
// 127.0.0.1. Each request is reported with its target authority and whether
// Proxy-Authorization arrived and matched proxyCredentials, never its value.

// The Basic credentials specs send: "proxy user" / "p@ss:word" percent-encoded
// in the proxy URL, so they exercise core's userinfo decoding.
const proxyCredentials = "Basic cHJveHkgdXNlcjpwQHNzOndvcmQ="

func proxyAuth(request *http.Request) string {
	switch request.Header.Get("Proxy-Authorization") {
	case "":
		return "none"
	case proxyCredentials:
		return "ok"
	default:
		return "wrong"
	}
}

// proxyTarget maps a .test host to 127.0.0.1, keeping the port.
func proxyTarget(authority string) string {
	host, port, err := net.SplitHostPort(authority)
	if err == nil && strings.HasSuffix(host, ".test") {
		return net.JoinHostPort("127.0.0.1", port)
	}
	return authority
}

func serveProxy() string {
	listener := newEventListener(listen("127.0.0.1:0"), "proxy", false)
	forward := &http.Transport{
		Proxy:             nil,
		DisableKeepAlives: true,
		DialContext: func(ctx context.Context, network, address string) (net.Conn, error) {
			return (&net.Dialer{}).DialContext(ctx, network, proxyTarget(address))
		},
	}
	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			go proxyConn(conn, forward)
		}
	}()
	return "http://" + listener.Addr().String()
}

func proxyConn(conn net.Conn, forward *http.Transport) {
	defer conn.Close()
	reader := bufio.NewReader(conn)
	for {
		request, err := http.ReadRequest(reader)
		if err != nil {
			return
		}
		if request.Method == http.MethodConnect {
			proxyTunnel(conn, reader, request)
			return
		}
		if !proxyForward(conn, request, forward) {
			return
		}
	}
}

func proxyTunnel(conn net.Conn, reader *bufio.Reader, request *http.Request) {
	emit("proxy connect %s auth=%s ua=%t", request.Host, proxyAuth(request), request.Header.Get("User-Agent") != "")
	upstream, err := net.Dial("tcp", proxyTarget(request.Host))
	if err != nil {
		_, _ = io.WriteString(conn, "HTTP/1.1 502 Bad Gateway\r\n\r\n")
		return
	}
	defer upstream.Close()
	if _, err := io.WriteString(conn, "HTTP/1.1 200 Connection established\r\n\r\n"); err != nil {
		return
	}
	done := make(chan struct{}, 2)
	go func() { _, _ = io.Copy(upstream, reader); done <- struct{}{} }()
	go func() { _, _ = io.Copy(conn, upstream); done <- struct{}{} }()
	<-done
}

// proxyForward relays one absolute-form request; false ends the connection.
func proxyForward(conn net.Conn, request *http.Request, forward *http.Transport) bool {
	emit("proxy forward %s %s auth=%s", request.Method, request.RequestURI, proxyAuth(request))
	outgoing := request.Clone(context.Background())
	outgoing.RequestURI = ""
	outgoing.Header.Del("Proxy-Authorization")
	response, err := forward.RoundTrip(outgoing)
	if err != nil {
		_, _ = io.WriteString(conn, "HTTP/1.1 502 Bad Gateway\r\nContent-Length: 0\r\nConnection: close\r\n\r\n")
		return false
	}
	defer response.Body.Close()
	response.Close = false
	return response.Write(conn) == nil
}
