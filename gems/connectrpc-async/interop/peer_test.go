package main

import (
	"context"
	"errors"
	"net"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"
)

// firstAccept signals when Serve first calls Accept, which follows its HTTP/2 setup.
type firstAccept struct {
	net.Listener
	once    sync.Once
	entered chan struct{}
}

func (l *firstAccept) Accept() (net.Conn, error) {
	l.once.Do(func() { close(l.entered) })
	return l.Listener.Accept()
}

// The interop scripts run this with -race. A TLSConfig read after launch usually
// runs before Serve's HTTP/2 setup, so only the race detector reliably reports it.
func TestStartServerAdvertisesCleartextAfterHTTP2Setup(t *testing.T) {
	inner, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	listener := &firstAccept{Listener: inner, entered: make(chan struct{})}
	protocols := new(http.Protocols)
	protocols.SetHTTP2(true)
	protocols.SetUnencryptedHTTP2(true)
	server := &http.Server{Protocols: protocols, ReadHeaderTimeout: 5 * time.Second,
		Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.ProtoMajor != 2 || r.TLS != nil {
				http.Error(w, "expected cleartext HTTP/2", http.StatusBadRequest)
				return
			}
			w.WriteHeader(http.StatusNoContent)
		})}
	origin, done := startServer(server, listener, nil)
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := server.Shutdown(ctx); err != nil {
			t.Error(err)
		}
		if err := <-done; !errors.Is(err, http.ErrServerClosed) {
			t.Errorf("Serve returned %v", err)
		}
	}()

	// The deadline only bounds a broken fixture; the Accept event orders the checks.
	select {
	case <-listener.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("Serve never reached its first Accept")
	}
	if server.TLSConfig == nil {
		t.Fatal("precondition: Go no longer installs TLSConfig for h2c; this regression no longer models the race")
	}
	if !strings.HasPrefix(origin, "http://") {
		t.Fatalf("h2c server advertised %q", origin)
	}

	transport := &http.Transport{Protocols: protocols}
	defer transport.CloseIdleConnections()
	response, err := (&http.Client{Transport: transport, Timeout: 5 * time.Second}).Get(origin)
	if err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	if response.StatusCode != http.StatusNoContent || response.ProtoMajor != 2 {
		t.Fatalf("advertised origin answered %s over HTTP/%d", response.Status, response.ProtoMajor)
	}
}
