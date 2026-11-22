package main

import (
	"bufio"
	"bytes"
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"net"
	"os"
	"strings"
	"time"

	pb "github.com/jimeh/connectrpc-ruby/interop/grpc/gen"
	"golang.org/x/net/http2"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/keepalive"
)

// Keepalive interop commands. The Ruby harness owns timing: these peers print
// progress lines on stdout and wait on stdin lines, never on fixed sleeps.
//
//	keepalive-serve mode fixtures         grpc-go server enforcing MinTime 1s
//	keepalive-client origin mode fixtures documented example client keepalive
//	keepalive-flood origin mode fixtures  raw framer sending PINGs back to back
//	keepalive-bdp origin mode fixtures    default client, large-payload streams
//	keepalive-age origin mode fixtures    bidi in flight across an age drain
//
// runKeepalive returns for any other command and exits after a keepalive one.
func runKeepalive(args []string) {
	commands := map[string]func(string, string, string){
		"keepalive-client": keepaliveClient,
		"keepalive-flood":  keepaliveFlood,
		"keepalive-bdp":    keepaliveBDP,
		"keepalive-age":    keepaliveAge,
	}
	if args[0] == "keepalive-serve" {
		check(len(args) == 3, "keepalive-serve mode fixtures")
		keepaliveServe(args[1], args[2])
		os.Exit(0)
	}
	command, ok := commands[args[0]]
	if !ok {
		return
	}
	check(len(args) == 4, args[0]+" origin mode fixtures")
	command(args[1], args[2], args[3])
	os.Exit(0)
}

// Scaled enforcement: grpc-go servers accept any MinTime, unlike clients.
func keepaliveServe(mode, dir string) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	must(err)
	options := []grpc.ServerOption{grpc.KeepaliveEnforcementPolicy(keepalive.EnforcementPolicy{
		MinTime: time.Second, PermitWithoutStream: false,
	})}
	scheme := "http"
	if mode != "h2c" {
		options = append(options, grpc.Creds(credentials.NewTLS(tlsConfig(mode, dir, "server"))))
		scheme = "https"
	}
	server := grpc.NewServer(options...)
	pb.RegisterPeerServer(server, &peer{})
	done := make(chan error, 1)
	go func() { done <- server.Serve(listener) }()
	fmt.Printf("%s://%s\n", scheme, listener.Addr())
	_, err = io.Copy(io.Discard, os.Stdin)
	must(err)
	server.Stop()
	must(<-done)
}

func keepaliveDial(origin, mode, dir string, options ...grpc.DialOption) *grpc.ClientConn {
	target := strings.TrimPrefix(strings.TrimPrefix(origin, "https://"), "http://")
	var creds credentials.TransportCredentials = insecure.NewCredentials()
	if mode != "h2c" {
		creds = credentials.NewTLS(tlsConfig(mode, dir, "client"))
	}
	options = append(options, grpc.WithTransportCredentials(creds), grpc.WithDisableRetry())
	connection, err := grpc.NewClient(target, options...)
	must(err)
	return connection
}

func keepaliveUnary(client pb.PeerClient, value string) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	reply, err := client.Unary(ctx, &pb.Message{Value: value})
	must(err)
	check(reply.Value == value, "unary reply")
}

var stdinLines = bufio.NewScanner(os.Stdin)

// Blocks until the harness writes the expected line.
func awaitLine(expected string) {
	check(stdinLines.Scan(), "stdin closed before "+expected)
	check(stdinLines.Text() == expected, "unexpected stdin line "+stdinLines.Text())
}

// The documented example client settings (grpc-go examples/features/keepalive).
// grpc-go raises any client Time below 10s to 10s, so the harness waits on
// real wall-clock PINGs at that external minimum.
func keepaliveClient(origin, mode, dir string) {
	connection := keepaliveDial(origin, mode, dir, grpc.WithKeepaliveParams(keepalive.ClientParameters{
		Time: 10 * time.Second, Timeout: time.Second, PermitWithoutStream: true,
	}))
	defer connection.Close()
	client := pb.NewPeerClient(connection)
	keepaliveUnary(client, "before")
	fmt.Println("READY")
	awaitLine("continue")
	keepaliveUnary(client, "after")
	fmt.Printf("PASS grpc-go client %s keepalive 10s/1s/permit quiet past the strike threshold, then unary\n", mode)
}

// A raw HTTP/2 peer that PINGs back to back on an idle connection, waiting for
// each ACK. With grpc-go's rule (the first PING never strikes, the third
// strike ends the connection) the GOAWAY follows the fourth PING.
func keepaliveFlood(origin, mode, dir string) {
	address := strings.TrimPrefix(strings.TrimPrefix(origin, "https://"), "http://")
	var conn net.Conn
	var err error
	if mode == "h2c" {
		conn, err = net.DialTimeout("tcp", address, 5*time.Second)
	} else {
		conn, err = tls.DialWithDialer(&net.Dialer{Timeout: 5 * time.Second}, "tcp", address, tlsConfig(mode, dir, "client"))
	}
	must(err)
	defer conn.Close()
	must(conn.SetDeadline(time.Now().Add(10 * time.Second)))
	_, err = io.WriteString(conn, http2.ClientPreface)
	must(err)
	framer := http2.NewFramer(conn, conn)
	must(framer.WriteSettings())
	payload := func(index int) [8]byte { return [8]byte{'f', 'l', 'o', 'o', 'd', 0, 0, byte(index)} }
	sent := 1
	must(framer.WritePing(false, payload(sent)))
	for {
		frame, err := framer.ReadFrame()
		must(err)
		switch frame := frame.(type) {
		case *http2.SettingsFrame:
			if !frame.IsAck() {
				must(framer.WriteSettingsAck())
			}
		case *http2.PingFrame:
			check(frame.IsAck() && frame.Data == payload(sent), "flood PING ACK")
			if sent < 4 {
				sent++
				must(framer.WritePing(false, payload(sent)))
			}
		case *http2.GoAwayFrame:
			check(sent == 4, fmt.Sprintf("GOAWAY after %d PINGs", sent))
			check(frame.ErrCode == http2.ErrCodeEnhanceYourCalm, "GOAWAY code "+frame.ErrCode.String())
			check(bytes.Equal(frame.DebugData(), []byte("too_many_pings")), "GOAWAY debug "+string(frame.DebugData()))
			check(frame.LastStreamID == 0, "GOAWAY last stream")
			fmt.Printf("PASS raw framer %s 4 back-to-back PINGs -> GOAWAY ENHANCE_YOUR_CALM too_many_pings\n", mode)
			return
		}
	}
}

// Default client options keep grpc-go's BDP estimator on, which PINGs as
// DATA arrives. Large payloads give it several samples.
func keepaliveBDP(origin, mode, dir string) {
	connection := keepaliveDial(origin, mode, dir)
	defer connection.Close()
	client := pb.NewPeerClient(connection)
	// A liveness guard, not a throughput bound: the checks are the BDP PING
	// evidence. With the host process pinned to 1/11 of a CPU, its 3 MiB
	// download and four 1 MiB echoes progressed steadily and took about 22 s
	// (#132).
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	payload := bytes.Repeat([]byte{0xA5}, 1<<20)
	ss, err := client.ServerStream(ctx, &pb.Message{Value: "bdp", Payload: payload})
	must(err)
	received := 0
	for {
		reply, err := ss.Recv()
		if err == io.EOF {
			break
		}
		must(err)
		check(bytes.Equal(reply.Payload, payload), "server stream payload")
		received++
	}
	check(received == 3, "server stream messages")
	bidi, err := client.Bidi(ctx)
	must(err)
	for index := range 4 {
		must(bidi.Send(&pb.Message{Value: fmt.Sprint(index), Payload: payload}))
		reply, err := bidi.Recv()
		must(err)
		check(reply.Value == fmt.Sprint(index) && bytes.Equal(reply.Payload, payload), "bidi echo")
	}
	must(bidi.CloseSend())
	_, err = bidi.Recv()
	check(err == io.EOF, "bidi EOF")
	fmt.Printf("PASS grpc-go default client %s 1 MiB server-stream and bidi with BDP PINGs\n", mode)
}

// A bidi opened before the server's age drain finishes after its GOAWAY;
// the next RPC needs a new connection.
func keepaliveAge(origin, mode, dir string) {
	connection := keepaliveDial(origin, mode, dir)
	defer connection.Close()
	client := pb.NewPeerClient(connection)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	bidi, err := client.Bidi(ctx)
	must(err)
	must(bidi.Send(&pb.Message{Value: "opened"}))
	reply, err := bidi.Recv()
	must(err)
	check(reply.Value == "opened", "bidi open echo")
	fmt.Println("OPEN")
	awaitLine("drained")
	must(bidi.Send(&pb.Message{Value: "after-goaway"}))
	reply, err = bidi.Recv()
	must(err)
	check(reply.Value == "after-goaway", "bidi echo after GOAWAY")
	must(bidi.CloseSend())
	_, err = bidi.Recv()
	check(err == io.EOF, "bidi EOF after GOAWAY")
	keepaliveUnary(client, "next")
	fmt.Printf("PASS grpc-go client %s bidi completed across the age drain, next unary\n", mode)
}
