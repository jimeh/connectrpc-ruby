package main

import (
	"context"
	"fmt"
	"io"
	"net"
	"strings"
	"time"

	pb "github.com/jimeh/connectrpc-ruby/interop/grpc/gen"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/status"
)

// The Peer service doubles as the health control channel. Its Unary values
// health:set:<service>:<STATUS>, health:watchers and health:shutdown run inside
// the server process that owns the client's connection.
const healthPeerService = "connectrpc.grpcinterop.v1.Peer"

type healthControlPeer struct {
	pb.UnimplementedPeerServer
	server *health.Server
}

// The reference server has no watcher count; the client skips that check.
func (p *healthControlPeer) Unary(_ context.Context, request *pb.Message) (*pb.Message, error) {
	switch {
	case strings.HasPrefix(request.Value, "health:set:"):
		fields := strings.Split(strings.TrimPrefix(request.Value, "health:set:"), ":")
		value, ok := healthpb.HealthCheckResponse_ServingStatus_value[fields[len(fields)-1]]
		if len(fields) != 2 || !ok {
			return nil, status.Error(codes.InvalidArgument, "health:set:<service>:<STATUS>")
		}
		p.server.SetServingStatus(fields[0], healthpb.HealthCheckResponse_ServingStatus(value))
	case request.Value == "health:shutdown":
		p.server.Shutdown()
	default:
		return nil, status.Error(codes.Unimplemented, "unsupported health control")
	}
	return request, nil
}

// runHealthCommand handles health-client and health-control; false means another command.
func runHealthCommand(command string, args []string) bool {
	switch command {
	case "health-client":
		check(len(args) == 3, "health-client origin mode fixtures")
		runHealthClient(args[0], args[1], args[2], false)
	case "health-control":
		check(len(args) == 2, "health-control mode fixtures")
		runHealthControl(args[0], args[1])
	default:
		return false
	}
	return true
}

func runHealthControl(mode, dir string) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	must(err)
	options := []grpc.ServerOption{}
	scheme := "http"
	if mode != "h2c" {
		options = append(options, grpc.Creds(credentials.NewTLS(tlsConfig(mode, dir, "server"))))
		scheme = "https"
	}
	server := grpc.NewServer(options...)
	reference := health.NewServer()
	reference.SetServingStatus(healthPeerService, healthpb.HealthCheckResponse_SERVING)
	healthpb.RegisterHealthServer(server, reference)
	pb.RegisterPeerServer(server, &healthControlPeer{server: reference})
	done := make(chan error, 1)
	go func() { done <- server.Serve(listener) }()
	runHealthClient(fmt.Sprintf("%s://%s", scheme, listener.Addr()), mode, dir, true)
	server.Stop()
	must(<-done)
}

type healthWatch struct {
	stream grpc.ServerStreamingClient[healthpb.HealthCheckResponse]
	cancel context.CancelFunc
}

func expectStatus(w healthWatch, want healthpb.HealthCheckResponse_ServingStatus, label string) {
	response, err := w.stream.Recv()
	must(err)
	check(response.Status == want, fmt.Sprintf("%s: got %s, want %s", label, response.Status, want))
}

// runHealthClient drives grpc-go's standard grpc_health_v1 client. Reference
// mode records grpc-go's documented differences: no watcher count, and after
// Shutdown it keeps Watch streams open and admits new watchers.
func runHealthClient(origin, mode, dir string, reference bool) {
	target := strings.TrimPrefix(strings.TrimPrefix(origin, "https://"), "http://")
	var creds credentials.TransportCredentials = insecure.NewCredentials()
	if mode != "h2c" {
		creds = credentials.NewTLS(tlsConfig(mode, dir, "client"))
	}
	connection, err := grpc.NewClient(target, grpc.WithTransportCredentials(creds), grpc.WithDisableRetry())
	must(err)
	defer connection.Close()
	client := healthpb.NewHealthClient(connection)
	peer := pb.NewPeerClient(connection)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	control := func(value string) string {
		reply, err := peer.Unary(ctx, &pb.Message{Value: value})
		must(err)
		return reply.Value
	}
	watch := func(service string) healthWatch {
		watchCtx, watchCancel := context.WithCancel(ctx)
		stream, err := client.Watch(watchCtx, &healthpb.HealthCheckRequest{Service: service})
		must(err)
		return healthWatch{stream: stream, cancel: watchCancel}
	}

	overall, err := client.Check(ctx, &healthpb.HealthCheckRequest{})
	must(err)
	check(overall.Status == healthpb.HealthCheckResponse_SERVING, "Check overall SERVING")
	registered, err := client.Check(ctx, &healthpb.HealthCheckRequest{Service: healthPeerService})
	must(err)
	check(registered.Status == healthpb.HealthCheckResponse_SERVING, "Check registered SERVING")
	_, err = client.Check(ctx, &healthpb.HealthCheckRequest{Service: "unknown.v1.Service"})
	check(status.Code(err) == codes.NotFound, "Check unknown NOT_FOUND")
	listed, err := client.List(ctx, &healthpb.HealthListRequest{})
	must(err)
	check(len(listed.Statuses) == 2 && listed.Statuses[""].Status == healthpb.HealthCheckResponse_SERVING &&
		listed.Statuses[healthPeerService].Status == healthpb.HealthCheckResponse_SERVING, "List snapshot")

	changes := watch(healthPeerService)
	expectStatus(changes, healthpb.HealthCheckResponse_SERVING, "Watch initial")
	control("health:set:" + healthPeerService + ":NOT_SERVING")
	expectStatus(changes, healthpb.HealthCheckResponse_NOT_SERVING, "Watch change")
	control("health:set:" + healthPeerService + ":SERVING")
	expectStatus(changes, healthpb.HealthCheckResponse_SERVING, "Watch change back")

	late := watch("late.v1.Service")
	expectStatus(late, healthpb.HealthCheckResponse_SERVICE_UNKNOWN, "Watch unknown")
	control("health:set:late.v1.Service:SERVING")
	expectStatus(late, healthpb.HealthCheckResponse_SERVING, "Watch registered later")
	late.cancel()
	changes.cancel()

	cancelled := watch("")
	expectStatus(cancelled, healthpb.HealthCheckResponse_SERVING, "Watch before cancel")
	cancelled.cancel()
	_, err = cancelled.stream.Recv()
	check(status.Code(err) == codes.Canceled, "cancelled Watch ends Canceled")
	if !reference {
		// Every earlier Watch was cancelled too; poll until the server released them.
		deadline := time.Now().Add(5 * time.Second)
		last := ""
		for last = control("health:watchers"); last != "0"; last = control("health:watchers") {
			check(time.Now().Before(deadline), "watchers back to baseline; last "+last)
			time.Sleep(10 * time.Millisecond)
		}
	}

	draining := watch(healthPeerService)
	expectStatus(draining, healthpb.HealthCheckResponse_SERVING, "Watch before shutdown")
	control("health:shutdown")
	expectStatus(draining, healthpb.HealthCheckResponse_NOT_SERVING, "Watch at shutdown")
	after := watch(healthPeerService)
	if reference {
		// grpc-go keeps the stream open after Shutdown and admits new watchers.
		expectStatus(after, healthpb.HealthCheckResponse_NOT_SERVING, "reference Watch after shutdown")
		after.cancel()
		draining.cancel()
	} else {
		_, err = draining.stream.Recv()
		check(err == io.EOF, "Watch ends OK after shutdown")
		_, err = after.stream.Recv()
		check(status.Code(err) == codes.Unavailable, "Watch after shutdown Unavailable")
	}
	server := "Ruby"
	if reference {
		server = "grpc-go reference"
	}
	fmt.Printf("PASS grpc-go health client vs %s %s Check/List/Watch change/unknown/cancel/shutdown\n", server, mode)
}
