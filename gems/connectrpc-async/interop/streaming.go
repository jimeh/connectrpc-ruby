// Independent Connect streaming peer. It uses the pinned real connect-go API.
package main

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

const prefix = "/interop.Streams/"

func must(err error) {
	if err != nil {
		panic(err)
	}
}
func check(ok bool, what string) {
	if !ok {
		panic(what)
	}
}
func credentials(directory string, mutual, server bool) *tls.Config {
	bytes, err := os.ReadFile(directory + "/ca.pem")
	must(err)
	roots := x509.NewCertPool()
	check(roots.AppendCertsFromPEM(bytes), "CA")
	config := &tls.Config{MinVersion: tls.VersionTLS12, NextProtos: []string{"h2"}, RootCAs: roots}
	if server || mutual {
		name := "client"
		if server {
			name = "server"
		}
		certificate, err := tls.LoadX509KeyPair(directory+"/"+name+".pem", directory+"/"+name+".key")
		must(err)
		config.Certificates = []tls.Certificate{certificate}
	}
	if server && mutual {
		config.ClientAuth = tls.RequireAndVerifyClientCert
		config.ClientCAs = roots
	}
	return config
}
func protocols() *http.Protocols {
	p := new(http.Protocols)
	p.SetHTTP2(true)
	p.SetUnencryptedHTTP2(true)
	return p
}

func serve(mode, directory string) {
	mux := http.NewServeMux()
	var lock sync.Mutex
	count := 0
	var release chan struct{}
	mux.Handle(prefix+"Control", connect.NewUnaryHandler(prefix+"Control", func(_ context.Context, r *connect.Request[wrapperspb.StringValue]) (*connect.Response[wrapperspb.StringValue], error) {
		lock.Lock()
		defer lock.Unlock()
		if r.Msg.Value == "release" {
			close(release)
			release = nil
		}
		return connect.NewResponse(wrapperspb.String(strconv.Itoa(count))), nil
	}))
	options := []connect.HandlerOption{connect.WithReadMaxBytes(1024)}
	mux.Handle(prefix+"SS", connect.NewServerStreamHandler(prefix+"SS", func(ctx context.Context, r *connect.Request[wrapperspb.StringValue], s *connect.ServerStream[wrapperspb.StringValue]) error {
		s.ResponseHeader().Set("X-Head", "early")
		s.ResponseTrailer().Set("X-Tail", "done")
		if r.Msg.Value == "error" {
			return connect.NewError(connect.CodeNotFound, errors.New("missing"))
		}
		if r.Msg.Value == "gate" {
			lock.Lock()
			release = make(chan struct{})
			gate := release
			lock.Unlock()
			must(s.Send(wrapperspb.String("first")))
			select {
			case <-gate:
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		return s.Send(r.Msg)
	}, options...))
	mux.Handle(prefix+"CS", connect.NewClientStreamHandler(prefix+"CS", func(_ context.Context, s *connect.ClientStream[wrapperspb.StringValue]) (*connect.Response[wrapperspb.StringValue], error) {
		lock.Lock()
		count = 0
		lock.Unlock()
		n := 0
		for s.Receive() {
			n++
			lock.Lock()
			count = n
			lock.Unlock()
		}
		if s.Err() != nil {
			return nil, s.Err()
		}
		r := connect.NewResponse(wrapperspb.String(strconv.Itoa(n)))
		r.Header().Set("X-Head", "early")
		r.Trailer().Set("X-Tail", "done")
		return r, nil
	}, options...))
	mux.Handle(prefix+"Bidi", connect.NewBidiStreamHandler(prefix+"Bidi", func(_ context.Context, s *connect.BidiStream[wrapperspb.StringValue, wrapperspb.StringValue]) error {
		s.ResponseHeader().Set("X-Head", "early")
		s.ResponseTrailer().Set("X-Tail", "done")
		for {
			m, err := s.Receive()
			if errors.Is(err, io.EOF) {
				return nil
			}
			if err != nil {
				return err
			}
			if err = s.Send(m); err != nil {
				return err
			}
		}
	}, options...))
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	must(err)
	server := &http.Server{Protocols: protocols(), ReadHeaderTimeout: 5 * time.Second, Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		check(r.ProtoMajor == 2, "server H2")
		if r.TLS != nil {
			check(r.TLS.NegotiatedProtocol == "h2", "ALPN")
			if mode == "mtls" {
				check(len(r.TLS.VerifiedChains) > 0, "mTLS")
			}
		}
		mux.ServeHTTP(w, r)
	})}
	scheme := "http"
	if mode != "h2c" {
		scheme = "https"
		server.TLSConfig = credentials(directory, mode == "mtls", true)
	}
	done := make(chan error, 1)
	go func() {
		if scheme == "https" {
			done <- server.ServeTLS(listener, "", "")
		} else {
			done <- server.Serve(listener)
		}
	}()
	fmt.Println(scheme + "://" + listener.Addr().String())
	bufio.NewScanner(os.Stdin).Scan()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	must(server.Shutdown(ctx))
	check(errors.Is(<-done, http.ErrServerClosed), "shutdown")
}

type checked struct{ transport *http.Transport }

func (c checked) RoundTrip(r *http.Request) (*http.Response, error) {
	response, err := c.transport.RoundTrip(r)
	if err == nil {
		check(response.ProtoMajor == 2, "client H2")
		if r.URL.Scheme == "https" {
			check(response.TLS.NegotiatedProtocol == "h2", "client ALPN")
		}
	}
	return response, err
}
func headers(h http.Header, coding string) { h.Set("Connect-Accept-Encoding", coding) }

// Raw malformed request counts complement the typed connect-go clients, whose
// server-stream API always emits exactly one request. This is real HTTP/2 I/O.
func requestCardinality(client *http.Client, origin, codec, coding string) {
	for _, count := range []int{0, 2} {
		payload := []byte{}
		if codec == "json" {
			payload = []byte(`""`)
		}
		flag := byte(0)
		if coding == "gzip" {
			var encoded bytes.Buffer
			writer := gzip.NewWriter(&encoded)
			_, err := writer.Write(payload)
			must(err)
			must(writer.Close())
			payload = encoded.Bytes()
			flag = 1
		}
		var body bytes.Buffer
		for range count {
			body.WriteByte(flag)
			must(binary.Write(&body, binary.BigEndian, uint32(len(payload))))
			body.Write(payload)
		}
		request, err := http.NewRequest("POST", origin+prefix+"SS", &body)
		must(err)
		request.Header.Set("Content-Type", "application/connect+"+codec)
		request.Header.Set("Connect-Protocol-Version", "1")
		request.Header.Set("Connect-Content-Encoding", coding)
		response, err := client.Do(request)
		must(err)
		check(response.StatusCode == 200, "cardinality HTTP status")
		wire, err := io.ReadAll(io.LimitReader(response.Body, 1025))
		must(err)
		must(response.Body.Close())
		check(len(wire) >= 5 && len(wire) <= 1024 && wire[0] == 2, "cardinality terminal")
		check(int(binary.BigEndian.Uint32(wire[1:5])) == len(wire)-5, "cardinality terminal tail")
		var terminal struct{ Error struct{ Code string } }
		must(json.Unmarshal(wire[5:], &terminal))
		check(terminal.Error.Code == "unimplemented", "cardinality code")
	}
}
func client(origin, mode, directory string) {
	transport := &http.Transport{Proxy: nil, DisableCompression: true, Protocols: protocols()}
	if mode != "h2c" {
		transport.TLSClientConfig = credentials(directory, mode == "mtls", false)
	}
	defer transport.CloseIdleConnections()
	httpClient := &http.Client{Transport: checked{transport}, Timeout: 5 * time.Second}
	for _, codec := range []string{"proto", "json"} {
		for _, coding := range []string{"identity", "gzip"} {
			options := []connect.ClientOption{}
			if codec == "json" {
				options = append(options, connect.WithProtoJSON())
			}
			if coding == "gzip" {
				options = append(options, connect.WithSendGzip())
			}
			makeClient := func(name string) *connect.Client[wrapperspb.StringValue, wrapperspb.StringValue] {
				return connect.NewClient[wrapperspb.StringValue, wrapperspb.StringValue](httpClient, origin+prefix+name, options...)
			}
			requestCardinality(httpClient, origin, codec, coding)
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			req := connect.NewRequest(wrapperspb.String("gate"))
			headers(req.Header(), coding)
			ss, err := makeClient("SS").CallServerStream(ctx, req)
			must(err)
			check(ss.Receive() && ss.Msg().Value == "first", "SS first before completion")
			check(ss.ResponseHeader().Get("X-Head") == "early", "SS headers")
			_, err = makeClient("Control").CallUnary(ctx, connect.NewRequest(wrapperspb.String("release")))
			must(err)
			check(ss.Receive() && ss.Msg().Value == "gate", "SS tail")
			check(!ss.Receive() && ss.Err() == nil, "SS EOF")
			check(ss.ResponseTrailer().Get("X-Tail") == "done", "SS trailers")
			must(ss.Close())
			cs := makeClient("CS").CallClientStream(ctx)
			headers(cs.RequestHeader(), coding)
			must(cs.Send(wrapperspb.String("")))
			for {
				progress, err := makeClient("Control").CallUnary(ctx, connect.NewRequest(wrapperspb.String("status")))
				must(err)
				if progress.Msg.Value == "1" {
					break
				}
			}
			must(cs.Send(wrapperspb.String("two")))
			response, err := cs.CloseAndReceive()
			must(err)
			check(response.Msg.Value == "2", "CS count")
			check(response.Trailer().Get("X-Tail") == "done", "CS trailers")
			zero := makeClient("CS").CallClientStream(ctx)
			headers(zero.RequestHeader(), coding)
			response, err = zero.CloseAndReceive()
			must(err)
			check(response.Msg.Value == "0", "CS zero input")
			bidi := makeClient("Bidi").CallBidiStream(ctx)
			headers(bidi.RequestHeader(), coding)
			must(bidi.Send(wrapperspb.String("")))
			first, err := bidi.Receive()
			must(err)
			check(first.Value == "", "bidi default message before halfclose")
			must(bidi.Send(wrapperspb.String("two")))
			second, err := bidi.Receive()
			must(err)
			check(second.Value == "two", "bidi second before halfclose")
			must(bidi.CloseRequest())
			_, err = bidi.Receive()
			check(errors.Is(err, io.EOF), "bidi EOF")
			check(bidi.ResponseTrailer().Get("X-Tail") == "done", "bidi trailers")
			must(bidi.CloseResponse())
			req = connect.NewRequest(wrapperspb.String("error"))
			headers(req.Header(), coding)
			ss, err = makeClient("SS").CallServerStream(ctx, req)
			must(err)
			check(!ss.Receive() && connect.CodeOf(ss.Err()) == connect.CodeNotFound, "SS early error")
			must(ss.Close())
			req = connect.NewRequest(wrapperspb.String(strings.Repeat("x", 2048)))
			headers(req.Header(), coding)
			ss, err = makeClient("SS").CallServerStream(ctx, req)
			must(err)
			check(!ss.Receive() && connect.CodeOf(ss.Err()) == connect.CodeResourceExhausted, "request bound")
			must(ss.Close())
			canceled, stop := context.WithCancel(ctx)
			bidi = makeClient("Bidi").CallBidiStream(canceled)
			headers(bidi.RequestHeader(), coding)
			must(bidi.Send(wrapperspb.String("progress")))
			_, err = bidi.Receive()
			must(err)
			stop()
			_, err = bidi.Receive()
			check(connect.CodeOf(err) == connect.CodeCanceled, "wire-progress cancellation")
			_ = bidi.CloseRequest()
			_ = bidi.CloseResponse()
			healthy := makeClient("CS").CallClientStream(ctx)
			headers(healthy.RequestHeader(), coding)
			response, err = healthy.CloseAndReceive()
			must(err)
			check(response.Msg.Value == "0", "healthy sibling after cancel")
			cancel()
			fmt.Printf("PASS Go->Ruby streaming %s/%s/%s SS/CS/bidi first-response/default/zero-input/incremental-CS/cardinality/headers/trailers/early-error/bounds/cancel/healthy\n", mode, codec, coding)
		}
	}
}
func main() {
	switch os.Args[1] {
	case "serve":
		serve(os.Args[2], os.Args[3])
	case "client":
		client(os.Args[2], os.Args[3], os.Args[4])
	default:
		panic("serve|client")
	}
}
