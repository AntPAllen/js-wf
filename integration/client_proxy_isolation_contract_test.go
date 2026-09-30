package integration_test

import (
	"bytes"
	"errors"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"js-wf/testcluster"
)

func TestClientProxyAsymmetricIsolationContract(t *testing.T) {
	_, cluster := setup(t)
	control := cluster.Clients[0]
	proxy, err := testcluster.NewClientProxy(cluster.Servers[0].ClientURL())
	if err != nil {
		t.Fatal(err)
	}
	defer proxy.Close()
	if err := proxy.EnableTrafficTrace(1 << 20); err != nil {
		t.Fatal(err)
	}
	worker, err := proxy.Connect()
	if err != nil {
		t.Fatal(err)
	}
	defer worker.Close()
	serverInbox := nats.NewInbox()
	request, err := control.SubscribeSync(serverInbox)
	if err != nil {
		t.Fatal(err)
	}
	replyInbox := nats.NewInbox()
	response, err := worker.SubscribeSync(replyInbox)
	if err != nil {
		t.Fatal(err)
	}
	if err := control.FlushTimeout(time.Second); err != nil {
		t.Fatal(err)
	}
	if err := worker.FlushTimeout(time.Second); err != nil {
		t.Fatal(err)
	}
	before := proxy.Stats()
	proxy.HoldResponses()
	defer proxy.ResumeResponses()
	if err := worker.PublishRequest(serverInbox, replyInbox, []byte("outbound")); err != nil {
		t.Fatal(err)
	}
	received, err := request.NextMsg(time.Second)
	if err != nil || string(received.Data) != "outbound" {
		t.Fatalf("outbound request: msg=%+v err=%v", received, err)
	}
	if err := control.Publish(received.Reply, []byte("inbound")); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(time.Second)
	for proxy.Stats().HeldBytes == before.HeldBytes && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	blocked := proxy.Stats()
	if !blocked.ResponsesHeld || blocked.HeldBytes <= before.HeldBytes || blocked.ClientToServer <= before.ClientToServer || blocked.Active != 1 {
		t.Fatalf("fault not confirmed: before=%+v blocked=%+v", before, blocked)
	}
	if _, err := response.NextMsg(20 * time.Millisecond); !errors.Is(err, nats.ErrTimeout) {
		t.Fatalf("inbound reply escaped isolation: %v", err)
	}
	proxy.ResumeResponses()
	reply, err := response.NextMsg(time.Second)
	if err != nil || string(reply.Data) != "inbound" {
		t.Fatalf("held reply after heal: msg=%+v err=%v", reply, err)
	}
	if err := worker.FlushTimeout(time.Second); err != nil {
		t.Fatal(err)
	}
	healed := proxy.Stats()
	if healed.ResponsesHeld || healed.ServerToClient <= blocked.ServerToClient || healed.Active != 1 {
		t.Fatalf("heal not confirmed: blocked=%+v healed=%+v", blocked, healed)
	}
	trace := proxy.TrafficTrace()
	if trace.Truncated || len(trace.Connections) != 1 || trace.Connections[0].Target == "" || trace.Connections[0].Upstream == "" {
		t.Fatalf("connection transcript: %+v", trace.Connections)
	}
	var outbound, inbound []byte
	for _, frame := range trace.Frames {
		if frame.Connection != trace.Connections[0].ID {
			t.Fatalf("frame belongs to an unrecorded connection: %d", frame.Connection)
		}
		switch frame.Direction {
		case "client_to_server":
			outbound = append(outbound, frame.Data...)
		case "server_to_client":
			inbound = append(inbound, frame.Data...)
		default:
			t.Fatalf("unknown transcript direction %q", frame.Direction)
		}
	}
	if !bytes.Contains(outbound, []byte("outbound\r\n")) || !bytes.Contains(inbound, []byte("inbound\r\n")) || !bytes.Contains(outbound, []byte("PING\r\n")) || !bytes.Contains(inbound, []byte("PONG\r\n")) {
		t.Fatal("transcript omitted forwarded request, reply or handshake")
	}
	// The consumer of a captured snapshot cannot modify the live transcript.
	first := trace.Frames[0].Data[0]
	trace.Frames[0].Data[0] ^= 0xff
	if proxy.TrafficTrace().Frames[0].Data[0] != first {
		t.Fatal("traffic snapshot shares mutable bytes with the live relay")
	}
	if err := proxy.EnableTrafficTrace(1024); err == nil {
		t.Fatal("accepted a new capture after the connection handshake")
	}

	limited, err := testcluster.NewClientProxy(cluster.Servers[0].ClientURL())
	if err != nil {
		t.Fatal(err)
	}
	defer limited.Close()
	if err := limited.EnableTrafficTrace(1024); err != nil {
		t.Fatal(err)
	}
	client, err := limited.Connect()
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	if err := client.FlushTimeout(time.Second); err != nil {
		t.Fatalf("exhausted diagnostic budget affected forwarding: %v", err)
	}
	if trace := limited.TrafficTrace(); !trace.Truncated || len(trace.Frames) != 0 {
		t.Fatal("exhausted capture was not reported as an incomplete transcript")
	}
}
