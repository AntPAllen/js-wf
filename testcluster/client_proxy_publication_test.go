package testcluster

import (
	"io"
	"net"
	"testing"
	"time"
)

func TestClientProxyExactPublicationSelector(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	proxy, err := NewClientProxy("nats://" + listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer proxy.Close()
	if err = proxy.HoldFirstPublication("wf.root.hash"); err != nil {
		t.Fatal(err)
	}
	client, err := net.Dial("tcp", proxy.listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	upstream, err := listener.Accept()
	if err != nil {
		t.Fatal(err)
	}
	defer upstream.Close()
	client.SetDeadline(time.Now().Add(3 * time.Second))
	upstream.SetDeadline(time.Now().Add(3 * time.Second))
	prefix := "PUB wf.root.hash.extra reply 0\r\n\r\nPUB $JS.API.INFO reply 2\r\n{}\r\n"
	packet := "PUB wf.root.hash reply 2\r\n{}\r\n"
	if _, err = client.Write([]byte(prefix + packet)); err != nil {
		t.Fatal(err)
	}
	got := make([]byte, len(prefix))
	if _, err = io.ReadFull(upstream, got); err != nil || string(got) != prefix {
		t.Fatal("sibling or metadata was held", string(got), err)
	}
	until := time.Now().Add(3 * time.Second)
	for proxy.PendingAPI() == nil && time.Now().Before(until) {
		time.Sleep(time.Millisecond)
	}
	held := proxy.PendingAPI()
	if held == nil || held.Subject != "wf.root.hash" || string(held.Packet) != packet || held.ForwardedBytes != 0 {
		t.Fatal("exact target not held", held)
	}
	proxy.ReleaseFirstAPI()
	got = make([]byte, len(packet))
	if _, err = io.ReadFull(upstream, got); err != nil || string(got) != packet {
		t.Fatal("held packet altered", string(got), err)
	}
}
