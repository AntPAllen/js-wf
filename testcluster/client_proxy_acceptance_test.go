package testcluster

import (
	"net"
	"strings"
	"testing"
	"time"
)

// An accepted client with a refused upstream produces no relayed connection or
// traffic. Offline proofs must inspect accepts, not just successful relays.
func TestClientProxyCountsFailedUpstreamDials(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	target := listener.Addr().String()
	if err = listener.Close(); err != nil {
		t.Fatal(err)
	}
	proxy, err := NewClientProxy("nats://" + target)
	if err != nil {
		t.Fatal(err)
	}
	defer proxy.Close()
	if err = proxy.EnableTrafficTrace(1024 * 1024); err != nil {
		t.Fatal(err)
	}
	client, err := net.DialTimeout("tcp", strings.TrimPrefix(proxy.URL(), "nats://"), time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	waitProxyStats(t, proxy, func(s ClientProxyStats) bool { return s.UpstreamDialFailures == 1 })
	proxy.Close()
	stats, trace := proxy.Stats(), proxy.TrafficTrace()
	if stats.AcceptedConnections != 1 || stats.UpstreamDialFailures != 1 || stats.Active != 0 || len(trace.Connections) != 0 || len(trace.Frames) != 0 {
		t.Fatalf("refused upstream must remain visible: stats=%+v connections=%d frames=%d", stats, len(trace.Connections), len(trace.Frames))
	}
}
