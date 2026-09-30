package testcluster

import (
	"bytes"
	"io"
	"net"
	"testing"
	"time"
)

func rawBufferedProxy(t *testing.T, limit int) (*ClientProxy, net.Conn, net.Conn) {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { listener.Close() })
	proxy, err := NewClientProxy("nats://" + listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	// Set before accepting any connection.
	proxy.mu.Lock()
	proxy.replyBufferLimit = limit
	proxy.mu.Unlock()
	t.Cleanup(proxy.Close)
	proxy.HoldResponses()
	client, err := net.Dial("tcp", proxy.listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { client.Close() })
	server, err := listener.Accept()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { server.Close() })
	client.SetDeadline(time.Now().Add(5 * time.Second))
	server.SetDeadline(time.Now().Add(5 * time.Second))
	return proxy, client, server
}

func waitProxyStats(t *testing.T, proxy *ClientProxy, ready func(ClientProxyStats) bool) ClientProxyStats {
	t.Helper()
	until := time.Now().Add(3 * time.Second)
	for time.Now().Before(until) {
		stats := proxy.Stats()
		if ready(stats) {
			return stats
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("proxy state did not converge: %+v", proxy.Stats())
	return ClientProxyStats{}
}

func TestReplyHoldDrainsUpstreamAndPreservesOrder(t *testing.T) {
	proxy, client, server := rawBufferedProxy(t, 8<<20)
	// Larger than the former relay's single 32 KiB read. The upstream must be
	// drained during the hold, with no byte yet forwarded to the client.
	payload := make([]byte, 2<<20)
	for i := range payload {
		payload[i] = byte(i*17 + i/251)
	}
	if _, err := server.Write(payload); err != nil {
		t.Fatal(err)
	}
	stats := waitProxyStats(t, proxy, func(s ClientProxyStats) bool { return s.BufferedBytes == uint64(len(payload)) })
	if stats.ServerToClient != 0 || stats.BufferOverflows != 0 || stats.HeldBytes != uint64(len(payload)) {
		t.Fatalf("held relay: %+v", stats)
	}
	if _, err := client.Write([]byte("request")); err != nil {
		t.Fatal(err)
	}
	request := make([]byte, 7)
	if _, err := io.ReadFull(server, request); err != nil || string(request) != "request" {
		t.Fatalf("outbound during hold: %q %v", request, err)
	}
	client.SetReadDeadline(time.Now().Add(20 * time.Millisecond))
	if _, err := client.Read(make([]byte, 1)); err == nil {
		t.Fatal("held response escaped")
	}
	client.SetReadDeadline(time.Now().Add(5 * time.Second))
	proxy.ResumeResponses()
	received := make([]byte, len(payload))
	if _, err := io.ReadFull(client, received); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(received, payload) {
		t.Fatal("buffer reordered or corrupted response bytes")
	}
	waitProxyStats(t, proxy, func(s ClientProxyStats) bool { return s.BufferedBytes == 0 && s.ServerToClient == uint64(len(payload)) })
	// A second hold must reuse the same connection without losing EOF-tail bytes.
	proxy.HoldResponses()
	if _, err := server.Write([]byte("tail")); err != nil {
		t.Fatal(err)
	}
	server.(*net.TCPConn).CloseWrite()
	waitProxyStats(t, proxy, func(s ClientProxyStats) bool { return s.BufferedBytes == 4 })
	proxy.ResumeResponses()
	tail, err := io.ReadAll(client)
	if err != nil || string(tail) != "tail" {
		t.Fatalf("EOF tail=%q err=%v", tail, err)
	}
	waitProxyStats(t, proxy, func(s ClientProxyStats) bool { return s.Active == 0 && s.BufferedBytes == 0 })
}

func TestReplyHoldOverflowIsExplicitAndCloseUnblocks(t *testing.T) {
	proxy, client, server := rawBufferedProxy(t, 4096)
	if _, err := server.Write(make([]byte, 8192)); err != nil {
		t.Fatal(err)
	}
	waitProxyStats(t, proxy, func(s ClientProxyStats) bool { return s.BufferOverflows == 1 && s.Active == 0 })
	if _, err := client.Read(make([]byte, 1)); err == nil {
		t.Fatal("overflow connection still usable")
	}
	proxy.ResumeResponses()
	if proxy.Stats().BufferOverflows != 1 {
		t.Fatal("heal erased fixture failure")
	}
	proxy.Close()
}

func TestReplyHoldCloseDiscardsBoundedBuffer(t *testing.T) {
	proxy, _, server := rawBufferedProxy(t, 1<<20)
	if _, err := server.Write(make([]byte, 65536)); err != nil {
		t.Fatal(err)
	}
	waitProxyStats(t, proxy, func(s ClientProxyStats) bool { return s.BufferedBytes == 65536 })
	done := make(chan struct{})
	go func() { proxy.Close(); close(done) }()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("close blocked on held response")
	}
	if s := proxy.Stats(); s.BufferedBytes != 0 || s.Active != 0 {
		t.Fatalf("buffer leak: %+v", s)
	}
}
