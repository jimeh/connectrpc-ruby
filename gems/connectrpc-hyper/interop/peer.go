package main

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"math/big"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"connectrpc.com/connect"
	pb "github.com/jimeh/connectrpc-ruby/interop/grpc/gen"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// ---------- shared behaviour, selected by the request message value ----------

func unaryBehaviour(ctx context.Context, tag string, in *pb.Message) (*pb.Message, error) {
	switch {
	case in.Value == "error" || in.Value == "bare-error":
		rich, err := status.New(codes.NotFound, "rich peer error").WithDetails(&pb.Message{Value: "opaque", Payload: []byte{0, 255}})
		must(err)
		return nil, rich.Err()
	case in.Value == "hang":
		emit("%s unary-hang-start", tag)
		<-ctx.Done()
		emit("%s unary-hang-done %v", tag, ctx.Err())
		return nil, status.FromContextError(ctx.Err()).Err()
	case strings.HasPrefix(in.Value, "big:"):
		n, _ := strconv.Atoi(strings.TrimPrefix(in.Value, "big:"))
		return &pb.Message{Value: "big", Payload: make([]byte, n)}, nil
	case strings.HasPrefix(in.Value, "once:"):
		emit("%s unary-handled %s", tag, in.Value)
	}
	return in, nil
}

func serverStreamBehaviour(ctx context.Context, tag string, in *pb.Message, send func(*pb.Message) error) error {
	switch {
	case strings.HasPrefix(in.Value, "count:"):
		parts := strings.Split(in.Value, ":")
		n, _ := strconv.Atoi(parts[1])
		size, _ := strconv.Atoi(parts[2])
		for i := 0; i < n; i++ {
			if err := send(&pb.Message{Value: strconv.Itoa(i), Payload: make([]byte, size)}); err != nil {
				emit("%s ss-count-aborted %d %v", tag, i, err)
				return err
			}
		}
		return nil
	case in.Value == "hold":
		if err := send(&pb.Message{Value: "m0"}); err != nil {
			return err
		}
		emit("%s ss-hold-sent", tag)
		<-ctx.Done()
		emit("%s ss-hold-done %v", tag, ctx.Err())
		return status.FromContextError(ctx.Err()).Err()
	case strings.HasPrefix(in.Value, "gate:"):
		// Sends m0, waits for "release <tag>", then sends m1 and ends cleanly.
		name := strings.TrimPrefix(in.Value, "gate:")
		if err := send(&pb.Message{Value: "m0"}); err != nil {
			return err
		}
		emit("%s ss-gate-sent %s", tag, name)
		select {
		case <-gate(name):
		case <-ctx.Done():
			emit("%s ss-gate-done %s %v", tag, name, ctx.Err())
			return status.FromContextError(ctx.Err()).Err()
		}
		err := send(&pb.Message{Value: "m1"})
		emit("%s ss-gate-finished %s %v", tag, name, err)
		return err
	case strings.HasPrefix(in.Value, "error-after:"):
		n, _ := strconv.Atoi(strings.TrimPrefix(in.Value, "error-after:"))
		for i := 0; i < n; i++ {
			if err := send(&pb.Message{Value: strconv.Itoa(i)}); err != nil {
				return err
			}
		}
		return status.Error(codes.ResourceExhausted, "late failure")
	case in.Value == "flood":
		// Sends 1 KiB messages until the client goes away; reports how far.
		for i := 0; ; i++ {
			if err := send(&pb.Message{Value: strconv.Itoa(i), Payload: make([]byte, 1024)}); err != nil {
				emit("%s ss-flood-done %d %v", tag, i, ctx.Err())
				return err
			}
		}
	}
	for _, value := range []string{in.Value, "second", ""} {
		if err := send(&pb.Message{Value: value, Payload: in.Payload}); err != nil {
			return err
		}
	}
	return nil
}

func clientStreamBehaviour(ctx context.Context, tag, mode string, recv func() (*pb.Message, error)) (*pb.Message, error) {
	if mode == "noread" {
		emit("%s cs-noread-start", tag)
		<-ctx.Done()
		emit("%s cs-noread-done %v", tag, ctx.Err())
		return nil, status.FromContextError(ctx.Err()).Err()
	}
	if name, ok := strings.CutPrefix(mode, "gate:"); ok {
		// Reads nothing until "release <name>", so the client's writes block.
		emit("%s cs-gate-start %s", tag, name)
		select {
		case <-gate(name):
		case <-ctx.Done():
			emit("%s cs-gate-done %s %v", tag, name, ctx.Err())
			return nil, status.FromContextError(ctx.Err()).Err()
		}
	}
	values := []string{}
	for {
		message, err := recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			emit("%s cs-error %v", tag, err)
			return nil, err
		}
		emit("%s cs-recv %s %d", tag, message.Value, len(message.Payload))
		values = append(values, message.Value)
	}
	emit("%s cs-eof %d", tag, len(values))
	return &pb.Message{Value: strings.Join(values, ",")}, nil
}

func bidiBehaviour(ctx context.Context, tag string, recv func() (*pb.Message, error), send func(*pb.Message) error) error {
	for {
		message, err := recv()
		if errors.Is(err, io.EOF) {
			emit("%s bidi-eof", tag)
			return nil
		}
		if err != nil {
			emit("%s bidi-done %v", tag, ctx.Err())
			return err
		}
		emit("%s bidi-recv %s", tag, message.Value)
		if name, ok := strings.CutPrefix(message.Value, "pause:"); ok {
			// Stops reading until "release <name>", so the client's writes block.
			emit("%s bidi-paused %s", tag, name)
			select {
			case <-gate(name):
			case <-ctx.Done():
				emit("%s bidi-done %v", tag, ctx.Err())
				return status.FromContextError(ctx.Err()).Err()
			}
			continue
		}
		if message.Value == "silent" {
			// Reads on without answering, so the client parks in a read.
			continue
		}
		if err = send(message); err != nil {
			return err
		}
	}
}

// ---------- grpc-go ----------

type grpcPeer struct {
	pb.UnimplementedPeerServer
	tag string
}

func (p *grpcPeer) Unary(ctx context.Context, request *pb.Message) (*pb.Message, error) {
	// "bare-error" sets no header metadata, so grpc-go answers trailers-only.
	if request.Value != "bare-error" {
		must(grpc.SetHeader(ctx, metadata.Pairs("x-head", "ready")))
	}
	must(grpc.SetTrailer(ctx, metadata.Pairs("x-tail", "done")))
	return unaryBehaviour(ctx, p.tag, request)
}

func (p *grpcPeer) ServerStream(request *pb.Message, stream grpc.ServerStreamingServer[pb.Message]) error {
	must(stream.SendHeader(metadata.Pairs("x-head", "ready")))
	stream.SetTrailer(metadata.Pairs("x-tail", "done"))
	return serverStreamBehaviour(stream.Context(), p.tag, request, stream.Send)
}

func (p *grpcPeer) ClientStream(stream grpc.ClientStreamingServer[pb.Message, pb.Message]) error {
	md, _ := metadata.FromIncomingContext(stream.Context())
	mode := strings.Join(md.Get("x-mode"), "")
	reply, err := clientStreamBehaviour(stream.Context(), p.tag, mode, stream.Recv)
	if err != nil {
		return err
	}
	return stream.SendAndClose(reply)
}

func (p *grpcPeer) Bidi(stream grpc.BidiStreamingServer[pb.Message, pb.Message]) error {
	return bidiBehaviour(stream.Context(), p.tag, stream.Recv, stream.Send)
}

func registerGRPC(server *grpc.Server, tag string) {
	pb.RegisterPeerServer(server, &grpcPeer{tag: tag})
}

func serveGRPC(tag string, config *tls.Config) string {
	listener := listen("127.0.0.1:0")
	options := []grpc.ServerOption{}
	scheme := "http"
	if config != nil {
		options = append(options, grpc.Creds(credentials.NewTLS(config)))
		scheme = "https"
	}
	server := grpc.NewServer(options...)
	registerGRPC(server, tag)
	go func() { _ = server.Serve(newEventListener(listener, tag, false)) }()
	return fmt.Sprintf("%s://%s", scheme, listener.Addr())
}

func serveGRPCWith(tag string, reads bool, options ...grpc.ServerOption) string {
	listener := listen("127.0.0.1:0")
	server := grpc.NewServer(options...)
	registerGRPC(server, tag)
	go func() { _ = server.Serve(newEventListener(listener, tag, reads)) }()
	return fmt.Sprintf("http://%s", listener.Addr())
}

// ---------- connect-go ----------

const procedurePrefix = "/connectrpc.grpcinterop.v1.Peer/"

func connectMux(tag string) *http.ServeMux {
	mux := http.NewServeMux()
	mux.Handle(procedurePrefix+"Unary", connect.NewUnaryHandler(procedurePrefix+"Unary",
		func(ctx context.Context, request *connect.Request[pb.Message]) (*connect.Response[pb.Message], error) {
			reply, err := unaryBehaviour(ctx, tag, request.Msg)
			if err != nil {
				return nil, toConnect(err)
			}
			response := connect.NewResponse(reply)
			response.Header().Set("x-head", "ready")
			response.Header().Set("x-method", request.HTTPMethod())
			response.Header().Set("x-timeout", request.Header().Get("Connect-Timeout-Ms"))
			response.Trailer().Set("x-tail", "done")
			return response, nil
		}, connect.WithIdempotency(connect.IdempotencyNoSideEffects)))
	mux.Handle(procedurePrefix+"ServerStream", connect.NewServerStreamHandler(procedurePrefix+"ServerStream",
		func(ctx context.Context, request *connect.Request[pb.Message], stream *connect.ServerStream[pb.Message]) error {
			stream.ResponseHeader().Set("x-head", "ready")
			stream.ResponseTrailer().Set("x-tail", "done")
			return toConnect(serverStreamBehaviour(ctx, tag, request.Msg, stream.Send))
		}))
	mux.Handle(procedurePrefix+"ClientStream", connect.NewClientStreamHandler(procedurePrefix+"ClientStream",
		func(ctx context.Context, stream *connect.ClientStream[pb.Message]) (*connect.Response[pb.Message], error) {
			recv := func() (*pb.Message, error) {
				if stream.Receive() {
					return stream.Msg(), nil
				}
				if err := stream.Err(); err != nil {
					return nil, err
				}
				return nil, io.EOF
			}
			reply, err := clientStreamBehaviour(ctx, tag, stream.RequestHeader().Get("x-mode"), recv)
			if err != nil {
				return nil, toConnect(err)
			}
			return connect.NewResponse(reply), nil
		}))
	mux.Handle(procedurePrefix+"Bidi", connect.NewBidiStreamHandler(procedurePrefix+"Bidi",
		func(ctx context.Context, stream *connect.BidiStream[pb.Message, pb.Message]) error {
			recv := func() (*pb.Message, error) { return stream.Receive() }
			return toConnect(bidiBehaviour(ctx, tag, recv, stream.Send))
		}))
	return mux
}

func toConnect(err error) error {
	if err == nil {
		return nil
	}
	if s, ok := status.FromError(err); ok {
		failure := connect.NewError(connect.Code(s.Code()), errors.New(s.Message()))
		for _, detail := range s.Details() {
			if message, ok := detail.(*pb.Message); ok {
				if converted, err := connect.NewErrorDetail(message); err == nil {
					failure.AddDetail(converted)
				}
			}
		}
		return failure
	}
	return err
}

// serveConnect serves HTTP/1.1 and h2c, or TLS with ALPN h2 and http/1.1. An
// h1only configuration offers ALPN http/1.1 alone and serves no HTTP/2.
func serveConnect(tag string, config *tls.Config) string {
	raw := listen("127.0.0.1:0")
	var listener net.Listener = newEventListener(raw, tag, false)
	mux := connectMux(tag)
	handler := http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		emit("%s http %s %s", tag, request.Proto, request.URL.Path)
		mux.ServeHTTP(writer, request)
	})
	server := &http.Server{Handler: handler, ReadHeaderTimeout: 5 * time.Second}
	protocols := new(http.Protocols)
	protocols.SetHTTP1(true)
	scheme := "http"
	if config != nil {
		if len(config.NextProtos) == 0 {
			protocols.SetHTTP2(true)
			config.NextProtos = []string{"h2", "http/1.1"}
		}
		server.TLSConfig = config
		scheme = "https"
		listener = tls.NewListener(listener, config)
	} else {
		protocols.SetUnencryptedHTTP2(true)
	}
	server.Protocols = protocols
	go func() { _ = server.Serve(listener) }()
	return fmt.Sprintf("%s://%s", scheme, raw.Addr())
}

// ---------- certificates ----------

func writePEM(path, kind string, der []byte) {
	file, err := os.Create(path)
	must(err)
	defer file.Close()
	must(pem.Encode(file, &pem.Block{Type: kind, Bytes: der}))
}

func issue(dir, name string, parent *x509.Certificate, parentKey *ecdsa.PrivateKey, template *x509.Certificate) (*x509.Certificate, *ecdsa.PrivateKey) {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	must(err)
	if parent == nil {
		parent, parentKey = template, key
	}
	der, err := x509.CreateCertificate(rand.Reader, template, parent, &key.PublicKey, parentKey)
	must(err)
	cert, err := x509.ParseCertificate(der)
	must(err)
	writePEM(filepath.Join(dir, name+".pem"), "CERTIFICATE", der)
	keyDER, err := x509.MarshalPKCS8PrivateKey(key)
	must(err)
	writePEM(filepath.Join(dir, name+".key"), "PRIVATE KEY", keyDER)
	return cert, key
}

func certificates(dir string) (*tls.Config, *tls.Config, *tls.Config) {
	now := time.Now()
	ca, caKey := issue(dir, "ca", nil, nil, &x509.Certificate{SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "hyper peer CA"},
		NotBefore: now.Add(-time.Hour), NotAfter: now.Add(24 * time.Hour), IsCA: true, BasicConstraintsValid: true,
		KeyUsage: x509.KeyUsageCertSign})
	issue(dir, "server", ca, caKey, &x509.Certificate{SerialNumber: big.NewInt(2), Subject: pkix.Name{CommonName: "localhost"},
		NotBefore: now.Add(-time.Hour), NotAfter: now.Add(24 * time.Hour), DNSNames: []string{"localhost", "peer.test"},
		IPAddresses: []net.IP{net.ParseIP("127.0.0.1")}, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		KeyUsage: x509.KeyUsageDigitalSignature})
	issue(dir, "client", ca, caKey, &x509.Certificate{SerialNumber: big.NewInt(3), Subject: pkix.Name{CommonName: "client"},
		NotBefore: now.Add(-time.Hour), NotAfter: now.Add(24 * time.Hour), ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
		KeyUsage: x509.KeyUsageDigitalSignature})
	pair, err := tls.LoadX509KeyPair(filepath.Join(dir, "server.pem"), filepath.Join(dir, "server.key"))
	must(err)
	pool := x509.NewCertPool()
	pool.AddCert(ca)
	server := &tls.Config{MinVersion: tls.VersionTLS12, Certificates: []tls.Certificate{pair}}
	mtls := server.Clone()
	mtls.ClientAuth = tls.RequireAndVerifyClientCert
	mtls.ClientCAs = pool
	h1only := server.Clone()
	h1only.NextProtos = []string{"http/1.1"}
	return server, mtls, h1only
}
