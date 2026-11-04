package main

import (
	"fmt"
	"io"
	"net"
	"testing"
	"time"
)

func TestHeaderGateCompleteSeparator(t *testing.T) {
	header := "POST / HTTP/1.1\r\nContent-Length: 4\r\n\r\n"
	for split := len(header) - 4; split <= len(header); split++ {
		t.Run(fmt.Sprintf("separator_byte_%d", split-(len(header)-4)), func(t *testing.T) {
			client, server := net.Pipe()
			defer client.Close()
			defer server.Close()
			// A deadline only bounds broken fixtures; successful progress uses reads
			// and the explicit dispatch event, with no timer releasing the gate.
			client.SetDeadline(time.Now().Add(2 * time.Second))
			server.SetDeadline(time.Now().Add(2 * time.Second))
			gate := &headerGate{Conn: client, release: make(chan struct{})}
			defer gate.resume(false)
			if _, bypass := any(gate).(io.ReaderFrom); bypass {
				t.Fatal("header gate exposes a ReadFrom bypass")
			}
			done := make(chan error, 1)
			go func() {
				if n, err := gate.Write([]byte(header[:split])); n != split || err != nil {
					done <- io.ErrShortWrite
					return
				}
				_, err := gate.Write([]byte(header[split:] + "body"))
				done <- err
			}()
			received := make([]byte, len(header))
			if _, err := io.ReadFull(server, received); err != nil {
				t.Fatal(err)
			}
			if string(received) != header || !gate.waiting.Load() {
				t.Fatal("incomplete header gate boundary")
			}
			select {
			case err := <-done:
				t.Fatalf("write returned before dispatch event: %v", err)
			default:
			}
			gate.resume(false)
			body := make([]byte, 4)
			if _, err := io.ReadFull(server, body); err != nil {
				t.Fatal(err)
			}
			if string(body) != "body" {
				t.Fatalf("coalesced body lost: %q", body)
			}
			if err := <-done; err != nil {
				t.Fatal(err)
			}
			if gate.early.Load() {
				t.Fatal("dispatch event marked early_head")
			}
		})
	}
}
