package integration_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"js-wf/journal"
	"js-wf/testcluster"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// Only the TCP relay changes transport behavior; errors and acknowledgments
// still come from the actual NATS client. Tail reads finish before the cut.
type journalNetworkCutJS struct {
	jetstream.JetStream
	proxy  *testcluster.ClientProxy
	commit bool
	held   chan struct{}
	before testcluster.ClientProxyStats
}

func (p *journalNetworkCutJS) PublishMsg(ctx context.Context, msg *nats.Msg, opts ...jetstream.PublishOpt) (*jetstream.PubAck, error) {
	p.cut()
	return p.JetStream.PublishMsg(ctx, msg, opts...)
}

func (p *journalNetworkCutJS) Publish(ctx context.Context, subject string, data []byte, opts ...jetstream.PublishOpt) (*jetstream.PubAck, error) {
	p.cut()
	return p.JetStream.Publish(ctx, subject, data, opts...)
}

func (p *journalNetworkCutJS) cut() {
	p.before = p.proxy.Stats()
	if p.commit {
		p.proxy.HoldResponses()
	} else {
		p.proxy.Block()
	}
	close(p.held)
}

// The default is the full 1,000-case requirement. A smaller explicit count is
// diagnostic only and cannot qualify it. Every attempt has a real TCP fault.
func TestThousandJournalNetworkLostAckRecoveries(t *testing.T) {
	if os.Getenv("WF_JOURNAL_NETWORK_ACK") != "1" {
		t.Skip("set WF_JOURNAL_NETWORK_ACK=1")
	}
	count := 1000
	if raw := os.Getenv("WF_JOURNAL_NETWORK_ACK_COUNT"); raw != "" {
		value, err := strconv.Atoi(raw)
		if err != nil || value < 2 || value > 1000 || value%2 != 0 {
			t.Fatal("invalid even network case count")
		}
		count = value
	}
	root := os.Getenv("WF_JOURNAL_NETWORK_ACK_REPORT")
	if root == "" {
		root = t.TempDir()
	}
	if err := os.MkdirAll(root, 0700); err != nil {
		t.Fatal(err)
	}
	save := func(name string, value any) {
		t.Helper()
		body, err := json.MarshalIndent(value, "", "  ")
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(root, name), append(body, '\n'), 0600); err != nil {
			t.Fatal(err)
		}
	}
	all, cluster := setup(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	stream, err := all[0].Stream(ctx, "WF_JRN")
	if err != nil {
		t.Fatal(err)
	}
	initial, err := stream.Info(ctx)
	if err != nil || initial.Cluster == nil {
		t.Fatalf("initial journal info: %v", err)
	}
	leader := -1
	for node, server := range cluster.Servers {
		if server.Name() == initial.Cluster.Leader {
			leader = node
		}
	}
	if leader < 0 {
		t.Fatal("unknown journal leader")
	}
	publisher, readerNode := (leader+1)%3, (leader+2)%3
	stream, err = all[readerNode].Stream(ctx, "WF_JRN")
	if err != nil {
		t.Fatal(err)
	}
	proxy, err := testcluster.NewClientProxy(cluster.Servers[publisher].ClientURL())
	if err != nil {
		t.Fatal(err)
	}
	defer proxy.Close()
	if err := proxy.EnableTrafficTrace(16 << 20); err != nil {
		t.Fatal(err)
	}
	defer func() { save("traffic.json", proxy.TrafficTrace()); save("proxy-final.json", proxy.Stats()) }()
	for i := 0; i < count; i++ {
		if i == count/2 {
			before, err := stream.Info(ctx)
			if err != nil || before.Cluster == nil || before.Cluster.Leader != initial.Cluster.Leader {
				t.Fatalf("selected leader changed before kill: info=%+v err=%v", before, err)
			}
			save("leader-before.json", before)
			cluster.KillNode(leader)
			moved := false
			until := time.Now().Add(20 * time.Second)
			for time.Now().Before(until) && ctx.Err() == nil {
				attempt, done := context.WithTimeout(ctx, time.Second)
				info, readErr := stream.Info(attempt)
				done()
				if readErr == nil && info.Cluster != nil && info.Cluster.Leader != "" && info.Cluster.Leader != initial.Cluster.Leader {
					save("leader-after-kill.json", info)
					moved = true
					break
				}
				time.Sleep(20 * time.Millisecond)
			}
			if !moved {
				t.Fatal("journal leader did not move after actual kill")
			}
			if err := cluster.RestartNode(leader); err != nil {
				t.Fatal(err)
			}
			all[leader], err = jetstream.New(cluster.Clients[leader])
			if err != nil {
				t.Fatal(err)
			}
		}
		id := fmt.Sprintf("network-ack-%04d", i)
		entry := journal.Entry{Epoch: 1, Index: 0, Kind: journal.Started, WorkerID: id}
		conn, err := nats.Connect(proxy.URL(), nats.NoReconnect(), nats.IgnoreDiscoveredServers())
		if err != nil {
			t.Fatal(err)
		}
		proxied, err := jetstream.New(conn)
		if err != nil {
			conn.Close()
			t.Fatal(err)
		}
		cut := &journalNetworkCutJS{JetStream: proxied, proxy: proxy, commit: i%2 == 0, held: make(chan struct{})}
		attempt, done := context.WithTimeout(ctx, 2*time.Second)
		result := make(chan error, 1)
		go func() { _, err := journal.New(cut).Append(attempt, "test", id, entry, 0); result <- err }()
		select {
		case <-cut.held:
		case err := <-result:
			done()
			conn.Close()
			t.Fatalf("case %d never reached publication: %v", i, err)
		case <-attempt.Done():
			done()
			conn.Close()
			t.Fatal("publish cut was not reached")
		}
		var first *jetstream.RawStreamMsg
		if cut.commit {
			for attempt.Err() == nil {
				first, err = stream.GetLastMsgForSubject(attempt, "wf.jrn.test."+id)
				if err == nil && proxy.Stats().HeldBytes > cut.before.HeldBytes {
					break
				}
				if err != nil && !errors.Is(err, jetstream.ErrMsgNotFound) {
					done()
					conn.Close()
					t.Fatal(err)
				}
				time.Sleep(time.Millisecond)
			}
			if first == nil || proxy.Stats().HeldBytes <= cut.before.HeldBytes {
				done()
				conn.Close()
				t.Fatalf("case %d lacks committed message and held response", i)
			}
			proxy.Block()
		} else {
			if _, err := stream.GetLastMsgForSubject(ctx, "wf.jrn.test."+id); !errors.Is(err, jetstream.ErrMsgNotFound) {
				done()
				conn.Close()
				t.Fatalf("case %d absent branch retained a message: %v", i, err)
			}
		}
		select {
		case err = <-result:
		case <-ctx.Done():
			done()
			conn.Close()
			t.Fatal("append did not finish")
		}
		done()
		conn.Close()
		if !errors.Is(err, journal.ErrUnknown) {
			t.Fatalf("case %d network loss returned %v", i, err)
		}
		unknown := err.Error()
		after := proxy.Stats()
		if after.BufferOverflows != 0 {
			t.Fatalf("invalid network fault evidence: before=%+v after=%+v", cut.before, after)
		}
		proxy.Heal()
		retry, retryErr := journal.New(all[readerNode]).Append(ctx, "test", id, entry, 0)
		if cut.commit && !errors.Is(retryErr, journal.ErrStale) || !cut.commit && retryErr != nil {
			t.Fatalf("case %d retry: %v", i, retryErr)
		}
		last, err := stream.GetLastMsgForSubject(ctx, "wf.jrn.test."+id)
		if err != nil {
			t.Fatal(err)
		}
		var decoded journal.Entry
		if err := journal.UnmarshalEntry(last.Data, &decoded); err != nil || decoded.WorkerID != id || decoded.Index != 0 || decoded.Epoch != 1 || decoded.Kind != journal.Started {
			t.Fatalf("case %d retained entry: %+v err=%v", i, decoded, err)
		}
		if cut.commit && (first.Sequence != last.Sequence || !bytes.Equal(first.Data, last.Data)) || !cut.commit && retry != last.Sequence {
			t.Fatal("recovery changed or duplicated the retained outcome")
		}
		save(fmt.Sprintf("case-%04d.json", i), map[string]any{"id": id, "committed": cut.commit, "unknown": unknown, "before": cut.before, "after": after, "first": first, "last": last, "retry_sequence": retry})
	}
	for node, js := range all {
		peer, err := js.Stream(ctx, "WF_JRN")
		if err != nil {
			t.Fatal(err)
		}
		info, err := peer.Info(ctx)
		if err != nil || info.State.Msgs != uint64(count) || info.State.NumSubjects != uint64(count) {
			t.Fatalf("node %d final journal census: %+v err=%v", node, info, err)
		}
		messages := make([]*jetstream.RawStreamMsg, 0, count)
		for i := 0; i < count; i++ {
			message, err := peer.GetLastMsgForSubject(ctx, fmt.Sprintf("wf.jrn.test.network-ack-%04d", i))
			if err != nil {
				t.Fatal(err)
			}
			body, err := os.ReadFile(filepath.Join(root, fmt.Sprintf("case-%04d.json", i)))
			if err != nil {
				t.Fatal(err)
			}
			var proof struct {
				Last *jetstream.RawStreamMsg `json:"last"`
			}
			if err := json.Unmarshal(body, &proof); err != nil {
				t.Fatal(err)
			}
			expected := proof.Last
			if expected == nil || message.Sequence != expected.Sequence || !bytes.Equal(message.Data, expected.Data) {
				t.Fatal("cross-peer retained outcome mismatch")
			}
			messages = append(messages, message)
		}
		save(fmt.Sprintf("peer-%d.json", node), info)
		save(fmt.Sprintf("peer-%d-messages.json", node), messages)
	}
	if proxy.TrafficTrace().Truncated {
		t.Fatal("network transcript truncated")
	}
	// Counter accounting can follow delivery of the preceding tail lookup.
	// Check the actual response bytes, rather than requiring zero metadata traffic.
	delivered := make(map[uint64][]byte)
	for _, frame := range proxy.TrafficTrace().Frames {
		if frame.Direction == "server_to_client" {
			delivered[frame.Connection] = append(delivered[frame.Connection], frame.Data...)
		}
	}
	for connection, data := range delivered {
		if bytes.Contains(data, []byte(`"stream":"WF_JRN"`)) {
			t.Fatalf("connection %d delivered the publish acknowledgment", connection)
		}
	}
	save("result.json", map[string]any{"cases": count, "committed": count / 2, "absent": count / 2, "leader_kills": 1, "cross_peer_checks": 3 * count, "full_thousand": count == 1000})
	t.Logf("JOURNAL_NETWORK_ACK cases=%d committed=%d absent=%d leader_kills=1 cross_peer_checks=%d full_thousand=%t", count, count/2, count/2, 3*count, count == 1000)
}
