package integration_test

import (
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
}
