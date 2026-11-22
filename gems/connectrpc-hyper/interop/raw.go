package main

import (
	"bytes"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
	"sync"

	"golang.org/x/net/http2"
	"golang.org/x/net/http2/hpack"
)

// A programmable HTTP/2 server for frame-exact cases grpc-go and connect-go
// never produce: header blocks of exact sizes, split trailers, server resets
// with any code, refused streams, withheld PING ACKs and silent streams. The
// "x-scenario" request header selects the response; every request, PING and
// client RST_STREAM is reported as an event.

type rawOptions struct {
	ack        bool
	maxStreams uint32
}

type rawConn struct {
	tag     string
	conn    net.Conn
	framer  *http2.Framer
	mu      sync.Mutex
	block   bytes.Buffer
	encoder *hpack.Encoder
	streams int
	last    uint32
	// awaiting holds await-ok streams until their request ends.
	awaiting map[uint32]*awaitedRequest
}

type awaitedRequest struct {
	scenario string
	grpc     bool
	bytes    int
}

// refusedOnce remembers the refuse-once and ignore-once keys already seen,
// across connections: a retry on a fresh connection then succeeds.
var refusedOnce = struct {
	sync.Mutex
	keys map[string]bool
}{keys: map[string]bool{}}

var rawConns = struct {
	sync.Mutex
	byTag map[string][]*rawConn
}{byTag: map[string][]*rawConn{}}

func serveRaw(tag string, options rawOptions) string {
	listener := newEventListener(listen("127.0.0.1:0"), tag, false)
	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			go handleRaw(tag, conn, options)
		}
	}()
	return fmt.Sprintf("http://%s", listener.Addr())
}

// rawGoaway sends GOAWAY(NO_ERROR) on each connection the listener has open,
// naming the last stream that connection has seen, or `last` when it is not
// negative: streams beyond it are never processed. With `closeAfter` it then
// ends that connection's write side (FIN, no reset), so its streams at or
// below the last stream id end without a response; connections accepted
// later are untouched.
func rawGoaway(tag string, last int64, closeAfter bool) {
	rawConns.Lock()
	conns := append([]*rawConn(nil), rawConns.byTag[tag]...)
	rawConns.Unlock()
	for _, c := range conns {
		c.mu.Lock()
		id := c.last
		if last >= 0 {
			id = uint32(last)
		}
		_ = c.framer.WriteGoAway(id, http2.ErrCodeNo, nil)
		if closeAfter {
			conn := c.conn
			if wrapped, ok := conn.(*eventConn); ok {
				conn = wrapped.Conn
			}
			if tcp, ok := conn.(*net.TCPConn); ok {
				_ = tcp.CloseWrite()
			}
		}
		c.mu.Unlock()
		emit("%s goaway-sent last=%d", c.tag, id)
	}
}

func handleRaw(tag string, conn net.Conn, options rawOptions) {
	defer conn.Close()
	preface := make([]byte, len(http2.ClientPreface))
	if _, err := io.ReadFull(conn, preface); err != nil || string(preface) != http2.ClientPreface {
		emit("%s bad-preface", tag)
		return
	}
	c := &rawConn{tag: tag, conn: conn, framer: http2.NewFramer(conn, conn),
		awaiting: map[uint32]*awaitedRequest{}}
	c.encoder = hpack.NewEncoder(&c.block)
	c.framer.ReadMetaHeaders = hpack.NewDecoder(4096, nil)
	c.framer.MaxHeaderListSize = 1 << 24
	settings := []http2.Setting{}
	if options.maxStreams > 0 {
		settings = append(settings, http2.Setting{ID: http2.SettingMaxConcurrentStreams, Val: options.maxStreams})
	}
	c.mu.Lock()
	must(c.framer.WriteSettings(settings...))
	c.mu.Unlock()
	rawConns.Lock()
	rawConns.byTag[tag] = append(rawConns.byTag[tag], c)
	rawConns.Unlock()
	defer func() {
		rawConns.Lock()
		list := rawConns.byTag[tag]
		for i, other := range list {
			if other == c {
				rawConns.byTag[tag] = append(list[:i], list[i+1:]...)
				break
			}
		}
		rawConns.Unlock()
	}()
	for {
		frame, err := c.framer.ReadFrame()
		if err != nil {
			return
		}
		switch frame := frame.(type) {
		case *http2.SettingsFrame:
			if !frame.IsAck() {
				c.locked(func() error { return c.framer.WriteSettingsAck() })
			}
		case *http2.PingFrame:
			if frame.IsAck() {
				continue
			}
			if options.ack {
				emit("%s ping %s", tag, conn.RemoteAddr())
				c.locked(func() error { return c.framer.WritePing(true, frame.Data) })
			} else {
				emit("%s ping-withheld %s", tag, conn.RemoteAddr())
			}
		case *http2.MetaHeadersFrame:
			c.request(frame)
		case *http2.DataFrame:
			if n := uint32(len(frame.Data())); n > 0 {
				c.locked(func() error {
					if err := c.framer.WriteWindowUpdate(0, n); err != nil {
						return err
					}
					return c.framer.WriteWindowUpdate(frame.StreamID, n)
				})
			}
			c.data(frame)
		case *http2.RSTStreamFrame:
			emit("%s rst stream=%d code=%d", tag, frame.StreamID, uint32(frame.ErrCode))
		case *http2.GoAwayFrame:
			emit("%s client-goaway code=%d", tag, uint32(frame.ErrCode))
		}
	}
}

func (c *rawConn) locked(write func() error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	_ = write()
}

// headers writes one field block, split into CONTINUATION frames as needed.
// Callers hold c.mu, so blocks never interleave.
func (c *rawConn) headers(stream uint32, fields [][2]string, end bool) {
	c.block.Reset()
	for _, field := range fields {
		must(c.encoder.WriteField(hpack.HeaderField{Name: field[0], Value: field[1]}))
	}
	block := c.block.Bytes()
	const chunk = 16384
	first := block
	if len(first) > chunk {
		first = block[:chunk]
	}
	rest := block[len(first):]
	_ = c.framer.WriteHeaders(http2.HeadersFrameParam{StreamID: stream, BlockFragment: first,
		EndHeaders: len(rest) == 0, EndStream: end})
	for len(rest) > 0 {
		next := rest
		if len(next) > chunk {
			next = rest[:chunk]
		}
		rest = rest[len(next):]
		_ = c.framer.WriteContinuation(stream, len(rest) == 0, next)
	}
}

var emptyMessage = []byte{0, 0, 0, 0, 0}

// fieldSize is core's GRPCProtocol.field_size for one field.
func fieldSize(name, value string) int { return len(name) + len(value) + 32 }

// padded returns fields plus an x-pad field sized so the block totals size.
func padded(fields [][2]string, size int, first bool) [][2]string {
	used := fieldSize("x-pad", "")
	for _, field := range fields {
		used += fieldSize(field[0], field[1])
	}
	pad := [2]string{"x-pad", strings.Repeat("p", max(size-used, 0))}
	if first {
		return append([][2]string{pad}, fields...)
	}
	return append(fields, pad)
}

// data counts an await-ok request's body and answers once it ends.
func (c *rawConn) data(frame *http2.DataFrame) {
	c.mu.Lock()
	defer c.mu.Unlock()
	awaited, ok := c.awaiting[frame.StreamID]
	if !ok {
		return
	}
	awaited.bytes += len(frame.Data())
	if frame.StreamEnded() {
		delete(c.awaiting, frame.StreamID)
		c.answer(frame.StreamID, awaited)
	}
}

// answer reports an await-ok request's body size and answers it. Callers
// hold c.mu.
func (c *rawConn) answer(stream uint32, awaited *awaitedRequest) {
	emit("%s handled %s stream=%d bytes=%d", c.tag, awaited.scenario, stream, awaited.bytes)
	if !awaited.grpc {
		c.headers(stream, [][2]string{{":status", "200"}, {"content-type", "application/proto"}}, true)
		return
	}
	c.headers(stream, [][2]string{{":status", "200"}, {"content-type", "application/grpc"}}, false)
	_ = c.framer.WriteData(stream, false, emptyMessage)
	c.headers(stream, [][2]string{{"grpc-status", "0"}}, true)
}

func (c *rawConn) request(frame *http2.MetaHeadersFrame) {
	values := map[string]string{}
	for _, field := range frame.Fields {
		values[field.Name] = field.Value
	}
	stream := frame.StreamID
	c.mu.Lock()
	defer c.mu.Unlock()
	c.streams++
	c.last = stream
	scenario := values["x-scenario"]
	grpc := strings.HasPrefix(values["content-type"], "application/grpc")
	emit("%s request %s stream=%d n=%d timeout=%s", c.tag, scenario, stream, c.streams,
		values["grpc-timeout"]+values["connect-timeout-ms"])
	head := [][2]string{{":status", "200"}, {"content-type", "application/grpc"}}
	ok := func() {
		emit("%s handled %s stream=%d", c.tag, scenario, stream)
		if !grpc {
			c.headers(stream, [][2]string{{":status", "200"}, {"content-type", "application/proto"}}, true)
			return
		}
		c.headers(stream, head, false)
		_ = c.framer.WriteData(stream, false, emptyMessage)
		c.headers(stream, [][2]string{{"grpc-status", "0"}}, true)
	}
	name, argument, _ := strings.Cut(scenario, ":")
	switch name {
	case "ok":
		ok()
	case "trailers-only":
		c.headers(stream, append(head, [2]string{"grpc-status", "5"}, [2]string{"grpc-message", "bare"}), true)
	case "split-trailers":
		c.headers(stream, head, false)
		c.headers(stream, [][2]string{{"grpc-status", "5"}, {"grpc-message", "split"}}, true)
	case "limit":
		// limit:<head|trailers>:<size>:<first|last> sizes that block's regular
		// fields to exactly <size> bytes, the pad first or last.
		parts := strings.Split(argument, ":")
		size, _ := strconv.Atoi(parts[1])
		first := parts[2] == "first"
		if parts[0] == "head" {
			c.headers(stream, append([][2]string{{":status", "200"}}, padded(head[1:], size, first)...), false)
			_ = c.framer.WriteData(stream, false, emptyMessage)
			c.headers(stream, [][2]string{{"grpc-status", "0"}}, true)
		} else {
			c.headers(stream, head, false)
			_ = c.framer.WriteData(stream, false, emptyMessage)
			c.headers(stream, padded([][2]string{{"grpc-status", "0"}}, size, !first), true)
		}
	case "reset-after":
		code, _ := strconv.Atoi(argument)
		c.headers(stream, head, false)
		_ = c.framer.WriteRSTStream(stream, http2.ErrCode(code))
	case "reset-before":
		code, _ := strconv.Atoi(argument)
		_ = c.framer.WriteRSTStream(stream, http2.ErrCode(code))
	case "reset-on":
		// reset-on:<gate>:<code> resets the stream without a response once
		// "release <gate>" arrives, as grpc-go's server resets a call whose
		// deadline expired.
		name, code, _ := strings.Cut(argument, ":")
		reset, _ := strconv.Atoi(code)
		go func() {
			<-gate(name)
			c.locked(func() error { return c.framer.WriteRSTStream(stream, http2.ErrCode(reset)) })
		}()
	case "refuse-once":
		// Refuses the first request carrying this key, on any connection.
		refusedOnce.Lock()
		first := !refusedOnce.keys[argument]
		refusedOnce.keys[argument] = true
		refusedOnce.Unlock()
		if first {
			_ = c.framer.WriteRSTStream(stream, http2.ErrCodeRefusedStream)
		} else {
			ok()
		}
	case "await-ok":
		// Answers once the whole request body arrived, reporting its size.
		awaited := &awaitedRequest{scenario: scenario, grpc: grpc}
		if frame.StreamEnded() {
			c.answer(stream, awaited)
		} else {
			c.awaiting[stream] = awaited
		}
	case "ignore-once":
		// Leaves the first request carrying this key unanswered, on any
		// connection: a GOAWAY naming an earlier stream then disowns it.
		refusedOnce.Lock()
		first := !refusedOnce.keys["ignore:"+argument]
		refusedOnce.keys["ignore:"+argument] = true
		refusedOnce.Unlock()
		if first {
			emit("%s ignored stream=%d", c.tag, stream)
		} else {
			ok()
		}
	case "stall":
		// Starts handling and never answers.
		emit("%s handler-start stream=%d", c.tag, stream)
	case "refuse-always":
		_ = c.framer.WriteRSTStream(stream, http2.ErrCodeRefusedStream)
	case "disconnect":
		emit("%s handler-start stream=%d", c.tag, stream)
		_ = c.conn.Close()
	case "hold":
		// Answers once "release <argument>" arrives.
		c.headers(stream, head, false)
		go func() {
			<-gate(argument)
			c.mu.Lock()
			defer c.mu.Unlock()
			_ = c.framer.WriteData(stream, false, emptyMessage)
			c.headers(stream, [][2]string{{"grpc-status", "0"}}, true)
		}()
	case "silent-data":
		c.headers(stream, head, false)
		_ = c.framer.WriteData(stream, false, emptyMessage)
	case "headers-only":
		c.headers(stream, head, false)
	default:
		c.headers(stream, [][2]string{{":status", "404"}}, true)
	}
}
