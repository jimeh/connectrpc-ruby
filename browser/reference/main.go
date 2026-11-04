// Development-only independent text reference. This module never ships in a gem.
package main

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"time"

	"connectrpc.com/connect"
	"github.com/improbable-eng/grpc-web/go/grpcweb"
	pb "github.com/jimeh/connectrpc-ruby/interop/grpc/gen"
	"google.golang.org/grpc"
)

type peer struct{ pb.UnimplementedPeerServer }

func (*peer) Unary(_ context.Context, request *pb.Message) (*pb.Message, error) {
	return request, nil
}
func (*peer) ServerStream(request *pb.Message, stream grpc.ServerStreamingServer[pb.Message]) error {
	if err := stream.Send(request); err != nil {
		return err
	}
	return stream.Send(&pb.Message{Value: "reference second"})
}
func main() {
	if len(os.Args) == 2 {
		binaryControl(os.Args[1])
		return
	}
	server := grpc.NewServer()
	pb.RegisterPeerServer(server, &peer{})
	web := grpcweb.WrapServer(server, grpcweb.WithOriginFunc(func(string) bool { return true }))
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		panic(err)
	}
	host := &http.Server{Handler: web}
	done := make(chan error, 1)
	go func() { done <- host.Serve(listener) }()
	fmt.Printf("http://%s\n", listener.Addr())
	if _, err := io.Copy(io.Discard, os.Stdin); err != nil {
		panic(err)
	}
	if err := host.Shutdown(context.Background()); err != nil {
		panic(err)
	}
	if err := <-done; err != http.ErrServerClosed {
		panic(err)
	}
	server.GracefulStop()
}

func binaryControl(origin string) {
	client := &http.Client{Timeout: 10 * time.Second}
	unary := connect.NewClient[pb.Message, pb.Message](client, origin+"/connectrpc.grpcinterop.v1.Peer/Unary", connect.WithGRPCWeb())
	response, err := unary.CallUnary(context.Background(), connect.NewRequest(&pb.Message{Value: "independent binary", Payload: []byte{0, 255}}))
	if err != nil || response.Msg.Value != "independent binary" || string(response.Msg.Payload) != string([]byte{0, 255}) {
		panic(fmt.Sprintf("independent binary unary: %v", err))
	}
	stream := connect.NewClient[pb.Message, pb.Message](client, origin+"/connectrpc.grpcinterop.v1.Peer/ServerStream", connect.WithGRPCWeb())
	request := connect.NewRequest(&pb.Message{Value: "binary-control"})
	result, err := stream.CallServerStream(context.Background(), request)
	if err != nil || !result.Receive() || result.Msg().Value != "first" {
		panic(fmt.Sprintf("independent first binary stream message: call=%v terminal=%v", err, result.Err()))
	}
	// The first actual message precedes release of the Ruby handler.
	release, err := client.Post(origin+"/__release?id=binary-control", "text/plain", nil)
	if err != nil {
		panic(err)
	}
	release.Body.Close()
	if !result.Receive() || result.Msg().Value != "second" || result.Receive() || result.Err() != nil {
		panic("independent binary stream did not finish with two typed messages and OK status")
	}
	result.Close()
	fmt.Println("PASS independent connect-go binary Web unary and incremental server-stream wire")
}
