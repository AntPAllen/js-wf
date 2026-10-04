//go:build linux

package integration_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"js-wf/provision"
)

type fiveUpgradeAPIObservation struct {
	Direction string      `json:"direction"`
	Subject   string      `json:"subject"`
	At        time.Time   `json:"at"`
	ElapsedNS int64       `json:"elapsed_ns"`
	Payload   []byte      `json:"payload,omitempty"`
	Header    nats.Header `json:"header,omitempty"`
}

// The rolling fixture uses default JetStream API options. This separate client
// shares that connection and traces only its provisioning call, not fleet work.
// Callbacks copy bytes in memory; artifact writes happen after the call returns.
func newFiveUpgradeProvisioningTrace(nc *nats.Conn) (jetstream.JetStream, func() []fiveUpgradeAPIObservation, error) {
	started := time.Now()
	var mu sync.Mutex
	var events []fiveUpgradeAPIObservation
	record := func(direction, subject string, payload []byte, header nats.Header) {
		mu.Lock()
		defer mu.Unlock()
		var copied nats.Header
		if header != nil {
			copied = make(nats.Header, len(header))
			for key, values := range header {
				copied[key] = append([]string(nil), values...)
			}
		}
		events = append(events, fiveUpgradeAPIObservation{Direction: direction, Subject: subject, At: time.Now().UTC(), ElapsedNS: time.Since(started).Nanoseconds(), Payload: append([]byte(nil), payload...), Header: copied})
	}
	js, err := jetstream.New(nc, jetstream.WithClientTrace(&jetstream.ClientTrace{
		RequestSent: func(subject string, payload []byte) { record("request", subject, payload, nil) },
		ResponseReceived: func(subject string, payload []byte, header nats.Header) {
			record("response", subject, payload, header)
		},
	}))
	return js, func() []fiveUpgradeAPIObservation {
		mu.Lock()
		defer mu.Unlock()
		return append([]fiveUpgradeAPIObservation(nil), events...)
	}, err
}

// A real NATS request gets one response, then the next metadata read gets none.
// The trace must expose that boundary and keep the original deadline cause.
func TestFiveUpgradeProvisioningTraceRetainsUnansweredRequest(t *testing.T) {
	s, err := server.NewServer(&server.Options{Host: "127.0.0.1", Port: -1})
	if err != nil {
		t.Fatal(err)
	}
	go s.Start()
	if !s.ReadyForConnections(2 * time.Second) {
		t.Fatal("server not ready")
	}
	t.Cleanup(func() { s.Shutdown(); s.WaitForShutdown() })
	nc, err := nats.Connect(s.ClientURL())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(nc.Close)
	requests := 0
	_, err = nc.Subscribe("$JS.API.STREAM.INFO.WF_RUN", func(msg *nats.Msg) {
		requests++
		if requests == 1 {
			_ = msg.Respond([]byte(`{"type":"io.nats.jetstream.api.v1.stream_info_response","config":{"name":"WF_RUN"},"state":{}}`))
		}
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := nc.Flush(); err != nil {
		t.Fatal(err)
	}
	js, snapshot, err := newFiveUpgradeProvisioningTrace(nc)
	if err != nil {
		t.Fatal(err)
	}
	ctx, stop := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer stop()
	check, err := checkFiveUpgradeFallback(ctx, func(operation context.Context) (provision.TimerBackend, error) {
		return provision.EnsureAuto(operation, js, 5)
	})
	if !errors.Is(err, context.DeadlineExceeded) || check.Error != context.DeadlineExceeded.Error() {
		t.Fatalf("cause changed: check=%+v err=%v", check, err)
	}
	events := snapshot()
	if len(events) != 3 || events[0].Direction != "request" || events[1].Direction != "response" || events[2].Direction != "request" {
		t.Fatalf("request/response boundary=%+v", events)
	}
	for index, event := range events {
		if event.Subject != "$JS.API.STREAM.INFO.WF_RUN" || event.At.IsZero() || event.ElapsedNS < 0 || (index > 0 && event.ElapsedNS < events[index-1].ElapsedNS) {
			t.Fatalf("invalid event=%+v", event)
		}
	}
	if len(events[1].Payload) == 0 {
		t.Fatal("response bytes absent")
	}
}

func TestFiveUpgradeNativeTraceRetainsUnansweredRequest(t *testing.T) {
	s, err := server.NewServer(&server.Options{Host: "127.0.0.1", Port: -1})
	if err != nil {
		t.Fatal(err)
	}
	go s.Start()
	if !s.ReadyForConnections(2 * time.Second) {
		t.Fatal("server not ready")
	}
	t.Cleanup(func() { s.Shutdown(); s.WaitForShutdown() })
	nc, err := nats.Connect(s.ClientURL())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(nc.Close)
	requests := 0
	_, err = nc.Subscribe("$JS.API.STREAM.INFO.WF_INV", func(msg *nats.Msg) {
		requests++
		if requests == 1 {
			_ = msg.Respond([]byte(`{"type":"io.nats.jetstream.api.v1.stream_info_response","config":{"name":"WF_INV"},"state":{}}`))
		}
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := nc.Flush(); err != nil {
		t.Fatal(err)
	}
	t.Setenv("WF_TIER3_UPGRADE_API_TRACE", "1")
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatal(err)
	}
	ctx, stop := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer stop()
	check, err := checkFiveUpgradeNativeProvisioning(ctx, nc.ConnectedServerVersion(), js)
	if !errors.Is(err, context.DeadlineExceeded) || check.Error != context.DeadlineExceeded.Error() {
		t.Fatalf("cause changed: check=%+v err=%v", check, err)
	}
	events := check.API
	if len(events) != 3 || events[0].Direction != "request" || events[1].Direction != "response" || events[2].Direction != "request" {
		t.Fatalf("request/response boundary=%+v", events)
	}
	for index, event := range events {
		if event.Subject != "$JS.API.STREAM.INFO.WF_INV" || event.At.IsZero() || event.ElapsedNS < 0 || (index > 0 && event.ElapsedNS < events[index-1].ElapsedNS) {
			t.Fatalf("invalid event=%+v", event)
		}
	}
	if len(events[1].Payload) == 0 {
		t.Fatal("response bytes absent")
	}
}
