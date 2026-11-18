package main

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/binary"
	"fmt"
	"net"
	"slices"
	"strings"
	"time"

	pb "github.com/jimeh/connectrpc-ruby/interop/grpc/gen"
	"golang.org/x/net/http2"
	"golang.org/x/net/http2/hpack"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

// runTrailersOnly proves the trailers-only case at frame level with a raw
// HTTP/2 client, then shows how grpc-go's own client reports it.
func runTrailersOnly(origin, mode, dir string) {
	trailersOnlyFrames(origin, mode, dir)
	trailersOnlyClient(origin, mode, dir)
}

// Each RPC must receive exactly one HEADERS frame on its stream, with
// END_HEADERS and END_STREAM, the status, details and trailer metadata, and
// no CONTINUATION, DATA or x-head.
func trailersOnlyFrames(origin, mode, dir string) {
	address := strings.TrimPrefix(strings.TrimPrefix(origin, "https://"), "http://")
	var conn net.Conn
	var err error
	scheme := "http"
	if mode == "h2c" {
		conn, err = net.DialTimeout("tcp", address, 5*time.Second)
	} else {
		scheme = "https"
		var secure *tls.Conn
		secure, err = tls.DialWithDialer(&net.Dialer{Timeout: 5 * time.Second}, "tcp", address, tlsConfig(mode, dir, "client"))
		if err == nil {
			check(secure.ConnectionState().NegotiatedProtocol == "h2", "trailers-only ALPN h2")
		}
		conn = secure
	}
	must(err)
	defer conn.Close()
	must(conn.SetDeadline(time.Now().Add(10 * time.Second)))
	_, err = conn.Write([]byte(http2.ClientPreface))
	must(err)
	framer := http2.NewFramer(conn, conn)
	framer.ReadMetaHeaders = hpack.NewDecoder(4096, nil)
	must(framer.WriteSettings())
	body, err := proto.Marshal(&pb.Message{Value: trailersOnly})
	must(err)
	envelope := binary.BigEndian.AppendUint32([]byte{0}, uint32(len(body)))
	envelope = append(envelope, body...)
	var block bytes.Buffer
	encoder := hpack.NewEncoder(&block)
	for index, method := range []string{"Unary", "ServerStream"} {
		streamID := uint32(2*index + 1)
		block.Reset()
		for _, field := range [][2]string{
			{":method", "POST"}, {":scheme", scheme}, {":path", "/connectrpc.grpcinterop.v1.Peer/" + method},
			{":authority", address}, {"content-type", "application/grpc"}, {"te", "trailers"},
		} {
			must(encoder.WriteField(hpack.HeaderField{Name: field[0], Value: field[1]}))
		}
		must(framer.WriteHeaders(http2.HeadersFrameParam{StreamID: streamID, BlockFragment: block.Bytes(), EndHeaders: true}))
		must(framer.WriteData(streamID, true, envelope))
		var received []string
		var fields []hpack.HeaderField
		for !slices.ContainsFunc(received, func(kind string) bool { return strings.HasSuffix(kind, "+END_STREAM") }) {
			frame, err := framer.ReadFrame()
			must(err)
			switch frame := frame.(type) {
			case *http2.SettingsFrame:
				if !frame.IsAck() {
					must(framer.WriteSettingsAck())
				}
			case *http2.PingFrame:
				if !frame.IsAck() {
					must(framer.WritePing(true, frame.Data))
				}
			case *http2.MetaHeadersFrame:
				if frame.StreamID == streamID {
					// ReadMetaHeaders folds CONTINUATION frames into this one.
					kind := "HEADERS"
					if !frame.HeadersEnded() {
						kind += "+CONTINUATION"
					}
					if frame.StreamEnded() {
						kind += "+END_STREAM"
					}
					received = append(received, kind)
					fields = frame.Fields
				}
			case *http2.DataFrame:
				if frame.StreamID == streamID {
					received = append(received, "DATA")
				}
			case *http2.RSTStreamFrame:
				panic(fmt.Sprintf("trailers-only %s RST_STREAM %s", method, frame.ErrCode))
			case *http2.GoAwayFrame:
				panic(fmt.Sprintf("trailers-only %s GOAWAY %s", method, frame.ErrCode))
			}
		}
		check(slices.Equal(received, []string{"HEADERS+END_STREAM"}), fmt.Sprintf("trailers-only %s frames %v", method, received))
		values := map[string][]string{}
		for _, field := range fields {
			values[field.Name] = append(values[field.Name], field.Value)
		}
		check(slices.Equal(values[":status"], []string{"200"}), "trailers-only :status")
		check(slices.Equal(values["content-type"], []string{"application/grpc"}), "trailers-only content-type")
		check(slices.Equal(values["grpc-status"], []string{"5"}), "trailers-only grpc-status")
		check(slices.Equal(values["grpc-message"], []string{"rich peer error"}), "trailers-only grpc-message")
		check(len(values["grpc-status-details-bin"]) == 1, "trailers-only grpc-status-details-bin")
		check(slices.Equal(values["x-tail"], []string{"done"}) && values["x-head"] == nil, "trailers-only metadata")
		fmt.Printf("PASS raw framer %s %s trailers-only: one HEADERS frame with END_HEADERS and END_STREAM, no CONTINUATION or DATA; status, details, x-tail\n", mode, method)
	}
}

// grpc-go's client reports no header metadata and puts the response
// metadata in the trailers, beside the typed details.
func trailersOnlyClient(origin, mode, dir string) {
	target := strings.TrimPrefix(strings.TrimPrefix(origin, "https://"), "http://")
	var creds credentials.TransportCredentials = insecure.NewCredentials()
	if mode != "h2c" {
		creds = credentials.NewTLS(tlsConfig(mode, dir, "client"))
	}
	connection, err := grpc.NewClient(target, grpc.WithTransportCredentials(creds), grpc.WithDisableRetry())
	must(err)
	defer connection.Close()
	client := pb.NewPeerClient(connection)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	var head, tail metadata.MD
	_, err = client.Unary(ctx, &pb.Message{Value: trailersOnly}, grpc.Header(&head), grpc.Trailer(&tail))
	checkRichError(err, "unary")
	check(len(head) == 0 && slices.Equal(tail.Get("x-tail"), []string{"done"}), "grpc-go trailers-only unary metadata")
	ss, err := client.ServerStream(ctx, &pb.Message{Value: trailersOnly})
	must(err)
	_, err = ss.Recv()
	checkRichError(err, "server stream")
	head, err = ss.Header()
	check(err == nil && len(head) == 0, "grpc-go trailers-only server stream header")
	check(slices.Equal(ss.Trailer().Get("x-tail"), []string{"done"}), "grpc-go trailers-only server stream trailer")
	fmt.Printf("PASS grpc-go client %s trailers-only unary/server-stream: no header metadata, x-tail trailer, typed details\n", mode)
}

func checkRichError(err error, label string) {
	rich, ok := status.FromError(err)
	check(ok && rich.Code() == codes.NotFound && rich.Message() == "rich peer error", "grpc-go trailers-only "+label+" status")
	details := rich.Details()
	check(len(details) == 1, "grpc-go trailers-only "+label+" details")
	detail, ok := details[0].(*pb.Message)
	check(ok && detail.Value == "opaque" && bytes.Equal(detail.Payload, []byte{0, 255}), "grpc-go trailers-only "+label+" detail")
}
