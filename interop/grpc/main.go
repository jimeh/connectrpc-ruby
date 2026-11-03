package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"strings"
	"time"

	pb "github.com/jimeh/connectrpc-ruby/interop/grpc/gen"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	_ "google.golang.org/grpc/encoding/gzip"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

type peer struct{ pb.UnimplementedPeerServer }

func (*peer) Unary(ctx context.Context, request *pb.Message) (*pb.Message, error) {
	grpc.SetHeader(ctx, metadata.Pairs("x-head", "ready"))
	grpc.SetTrailer(ctx, metadata.Pairs("x-tail", "done"))
	if request.Value == "error" {
		rich, err := status.New(codes.NotFound, "rich peer error").WithDetails(&pb.Message{Value: "opaque", Payload: []byte{0, 255}})
		if err != nil {
			return nil, err
		}
		return nil, rich.Err()
	}
	return request, nil
}
func (*peer) ServerStream(request *pb.Message, stream grpc.ServerStreamingServer[pb.Message]) error {
	if err := stream.SendHeader(metadata.Pairs("x-head", "ready")); err != nil {
		return err
	}
	stream.SetTrailer(metadata.Pairs("x-tail", "done"))
	for _, value := range []string{request.Value, "second", ""} {
		if err := stream.Send(&pb.Message{Value: value, Payload: request.Payload}); err != nil {
			return err
		}
	}
	return nil
}
func (*peer) ClientStream(stream grpc.ClientStreamingServer[pb.Message, pb.Message]) error {
	values := []string{}
	for {
		message, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		values = append(values, message.Value)
	}
	return stream.SendAndClose(&pb.Message{Value: strings.Join(values, ",")})
}
func (*peer) Bidi(stream grpc.BidiStreamingServer[pb.Message, pb.Message]) error {
	for {
		message, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		if err = stream.Send(message); err != nil {
			return err
		}
	}
}
func tlsConfig(mode, dir, role string) *tls.Config {
	root, err := os.ReadFile(filepath.Join(dir, "ca.pem"))
	must(err)
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(root) {
		panic("CA")
	}
	config := &tls.Config{MinVersion: tls.VersionTLS12, NextProtos: []string{"h2"}, RootCAs: pool}
	if role == "server" || mode == "mtls" {
		pair, err := tls.LoadX509KeyPair(filepath.Join(dir, role+".pem"), filepath.Join(dir, role+".key"))
		must(err)
		config.Certificates = []tls.Certificate{pair}
	}
	if role == "server" && mode == "mtls" {
		config.ClientAuth = tls.RequireAndVerifyClientCert
		config.ClientCAs = pool
	}
	return config
}
func must(err error) {
	if err != nil {
		panic(err)
	}
}
func check(condition bool, message string) {
	if !condition {
		panic(message)
	}
}
func runClient(origin, mode, dir string) {
	target := strings.TrimPrefix(strings.TrimPrefix(origin, "https://"), "http://")
	var creds credentials.TransportCredentials = insecure.NewCredentials()
	if mode != "h2c" {
		creds = credentials.NewTLS(tlsConfig(mode, dir, "client"))
	}
	connection, err := grpc.NewClient(target, grpc.WithTransportCredentials(creds), grpc.WithDisableRetry())
	must(err)
	defer connection.Close()
	client := pb.NewPeerClient(connection)
	for _, coding := range []string{"identity", "gzip"} {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		options := []grpc.CallOption{}
		if coding == "gzip" {
			options = append(options, grpc.UseCompressor("gzip"))
		}
		var head, tail metadata.MD
		callOptions := append(append([]grpc.CallOption{}, options...), grpc.Header(&head), grpc.Trailer(&tail))
		message := &pb.Message{Value: "unary", Payload: []byte{0, 255}}
		reply, err := client.Unary(ctx, message, callOptions...)
		must(err)
		check(reply.Value == message.Value && string(reply.Payload) == string(message.Payload), "unary reply")
		check(len(head.Get("x-head")) == 1 && len(tail.Get("x-tail")) == 1, "unary metadata")
		ss, err := client.ServerStream(ctx, &pb.Message{Value: "first"}, options...)
		must(err)
		values := []string{}
		for {
			reply, err := ss.Recv()
			if err == io.EOF {
				break
			}
			must(err)
			values = append(values, reply.Value)
		}
		check(strings.Join(values, "|") == "first|second|", "server stream")
		cs, err := client.ClientStream(ctx, options...)
		must(err)
		must(cs.Send(&pb.Message{Value: "a"}))
		must(cs.Send(&pb.Message{Value: "b"}))
		reply, err = cs.CloseAndRecv()
		must(err)
		check(reply.Value == "a,b", "client stream")
		bidi, err := client.Bidi(ctx, options...)
		must(err)
		must(bidi.Send(&pb.Message{Value: "chat"}))
		reply, err = bidi.Recv()
		must(err)
		check(reply.Value == "chat", "incremental bidi")
		must(bidi.CloseSend())
		_, err = bidi.Recv()
		check(err == io.EOF, "bidi EOF")
		cancel()
		fmt.Printf("PASS grpc-go client %s/%s unary/server-stream/client-stream/bidi metadata/halfclose\n", mode, coding)
	}
}
func runNegative(origin, mode, dir, scenario string) {
	target := strings.TrimPrefix(origin, "https://")
	config := tlsConfig(mode, dir, "client")
	switch scenario {
	case "untrusted-server":
		config.RootCAs = x509.NewCertPool()
	case "hostname":
		config.ServerName = "elsewhere.invalid"
	case "missing-client":
		config.Certificates = nil
	case "untrusted-client":
		pair, err := tls.LoadX509KeyPair(filepath.Join(dir, "untrusted-client.pem"), filepath.Join(dir, "untrusted-client.key"))
		must(err)
		config.Certificates = []tls.Certificate{pair}
	default:
		panic("unknown negative scenario")
	}
	connection, err := grpc.NewClient(target, grpc.WithTransportCredentials(credentials.NewTLS(config)), grpc.WithDisableRetry())
	must(err)
	defer connection.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_, err = pb.NewPeerClient(connection).Unary(ctx, &pb.Message{Value: "negative"})
	check(status.Code(err) == codes.Unavailable, "TLS negative must fail before RPC")
	fmt.Printf("PASS grpc-go TLS negative %s/%s\n", mode, scenario)
}
func main() {
	check(len(os.Args) >= 2, "serve|client|control")
	command := os.Args[1]
	if command == "negative" {
		check(len(os.Args) == 6, "negative origin mode fixtures scenario")
		runNegative(os.Args[2], os.Args[3], os.Args[4], os.Args[5])
		return
	}
	if command == "client" {
		check(len(os.Args) == 5, "client origin mode fixtures")
		runClient(os.Args[2], os.Args[3], os.Args[4])
		return
	}
	check(command == "serve" || command == "control", "command")
	check(len(os.Args) == 4, "serve|control mode fixtures")
	mode, dir := os.Args[2], os.Args[3]
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	must(err)
	options := []grpc.ServerOption{}
	scheme := "http"
	if mode != "h2c" {
		options = append(options, grpc.Creds(credentials.NewTLS(tlsConfig(mode, dir, "server"))))
		scheme = "https"
	}
	server := grpc.NewServer(options...)
	pb.RegisterPeerServer(server, &peer{})
	done := make(chan error, 1)
	go func() { done <- server.Serve(listener) }()
	origin := fmt.Sprintf("%s://%s", scheme, listener.Addr())
	if command == "control" {
		runClient(origin, mode, dir)
	} else {
		fmt.Println(origin)
		_, err = io.Copy(io.Discard, os.Stdin)
		must(err)
	}
	server.GracefulStop()
	must(<-done)
}
