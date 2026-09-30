package integration_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"js-wf/client"
	"js-wf/history"
	"js-wf/identity"
	"js-wf/journal"
	"js-wf/provision"
	"js-wf/reconcile"
	"js-wf/testcluster"

	"github.com/anishathalye/porcupine"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

func TestConcurrentEnqueueSameMessageID(t *testing.T) {
	all, _ := setup(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	const callers = 64
	var ready sync.WaitGroup
	ready.Add(callers)
	start := make(chan struct{})
	results := make(chan error, callers)
	for i := 0; i < callers; i++ {
		go func(js jetstream.JetStream) {
			ready.Done()
			<-start
			results <- client.New(js).Enqueue(ctx, "test", "same-wakeup", "same-message-id")
		}(all[i%len(all)])
	}
	ready.Wait()
	close(start)
	for range callers {
		if err := <-results; err != nil {
			t.Fatalf("concurrent same-ID enqueue: %v", err)
		}
	}
	subject := identity.RunSubject("test", "same-wakeup", provision.Partitions)
	for i, js := range all {
		run, err := js.Stream(ctx, "WF_RUN")
		if err != nil {
			t.Fatal(err)
		}
		raw, err := run.GetLastMsgForSubject(ctx, subject)
		if err != nil || string(raw.Data) != identity.Key("test", "same-wakeup") {
			t.Fatalf("node %d retained wakeup=%+v err=%v", i, raw, err)
		}
		info, err := run.Info(ctx)
		if err != nil || info.State.Msgs != 1 {
			t.Fatalf("node %d retained run messages=%+v err=%v", i, info, err)
		}
	}
}

// ackLossJS simulates a lost publish acknowledgment at the SDK boundary. A
// successful underlying publish is committed on the real three-node server;
// the caller only sees a timeout. dropBeforePublish covers the absent branch.
type ackLossJS struct {
	jetstream.JetStream
	subject           string
	dropBeforePublish bool
	lostErr           error
	fired             atomic.Bool
}

type delayedStartReadJS struct {
	jetstream.JetStream
	reads atomic.Int32
}

func (d *delayedStartReadJS) Stream(ctx context.Context, name string) (jetstream.Stream, error) {
	stream, err := d.JetStream.Stream(ctx, name)
	if err != nil || name != "WF_INV" {
		return stream, err
	}
	return &delayedStartStream{Stream: stream, reads: &d.reads}, nil
}

type delayedStartStream struct {
	jetstream.Stream
	reads *atomic.Int32
}

func (d *delayedStartStream) GetLastMsgForSubject(ctx context.Context, subject string) (*jetstream.RawStreamMsg, error) {
	if d.reads.Add(1) <= 3 {
		return nil, jetstream.ErrMsgNotFound
	}
	return d.Stream.GetLastMsgForSubject(ctx, subject)
}

func TestConcurrentStartRetriesTransientReplicaRead(t *testing.T) {
	all, _ := setup(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	base := client.New(all[0])
	first, err := base.Start(ctx, "test", "delayed-start-read", []byte(`1`))
	if err != nil {
		t.Fatal(err)
	}
	delayed := &delayedStartReadJS{JetStream: all[1]}
	second, err := client.New(delayed).Start(ctx, "test", "delayed-start-read", []byte(`1`))
	if !errors.Is(err, client.ErrAlreadyStarted) || second.InvSeq != first.InvSeq || delayed.reads.Load() != 4 {
		t.Fatalf("retry: first=%+v second=%+v reads=%d err=%v", first, second, delayed.reads.Load(), err)
	}
	absent := &ackLossJS{JetStream: all[1], subject: "wf.inv.test.start-never-committed", dropBeforePublish: true}
	shortCtx, shortCancel := context.WithTimeout(ctx, 250*time.Millisecond)
	defer shortCancel()
	handle, err := client.New(absent).Start(shortCtx, "test", "start-never-committed", []byte(`1`))
	if !errors.Is(err, client.ErrStartUnknown) || handle.InvSeq != 0 {
		t.Fatalf("absent publish: handle=%+v err=%v", handle, err)
	}
	inv, err := all[0].Stream(ctx, "WF_INV")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := inv.GetLastMsgForSubject(ctx, absent.subject); !errors.Is(err, jetstream.ErrMsgNotFound) {
		t.Fatalf("absent invocation was stored: %v", err)
	}
}

func TestStartRetriesAbsentPublishWithSameInput(t *testing.T) {
	all, _ := setup(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	const typ, id = "test", "retry-absent-start"
	firstLost := &ackLossJS{JetStream: all[0], subject: "wf.inv." + typ + "." + id, dropBeforePublish: true, lostErr: nats.ErrTimeout}
	handle, err := client.New(firstLost).Start(ctx, typ, id, []byte(`123`))
	if err != nil || handle.InvSeq == 0 || !firstLost.fired.Load() {
		t.Fatalf("absent publish retry: handle=%+v dropped=%v err=%v", handle, firstLost.fired.Load(), err)
	}
	inv, err := all[1].Stream(ctx, "WF_INV")
	if err != nil {
		t.Fatal(err)
	}
	stored, err := inv.GetLastMsgForSubject(ctx, firstLost.subject)
	if err != nil || stored.Sequence != handle.InvSeq || string(stored.Data) != "123" {
		t.Fatalf("retried invocation: message=%+v err=%v", stored, err)
	}
	info, err := inv.Info(ctx)
	if err != nil || info.State.Msgs != 1 {
		t.Fatalf("invocation count: info=%+v err=%v", info, err)
	}
	run, err := all[1].Stream(ctx, "WF_RUN")
	if err != nil {
		t.Fatal(err)
	}
	info, err = run.Info(ctx)
	if err != nil || info.State.Msgs != 1 {
		t.Fatalf("run count: info=%+v err=%v", info, err)
	}
}

type holdJournalResponseJS struct {
	jetstream.JetStream
	proxy *testcluster.ClientProxy
	held  chan struct{}
}

func (h *holdJournalResponseJS) Publish(ctx context.Context, subject string, payload []byte, opts ...jetstream.PublishOpt) (*jetstream.PubAck, error) {
	h.proxy.HoldResponses()
	close(h.held)
	return h.JetStream.Publish(ctx, subject, payload, opts...)
}

func (h *holdJournalResponseJS) PublishMsg(ctx context.Context, msg *nats.Msg, opts ...jetstream.PublishOpt) (*jetstream.PubAck, error) {
	h.proxy.HoldResponses()
	close(h.held)
	return h.JetStream.PublishMsg(ctx, msg, opts...)
}

func (a *ackLossJS) hiddenAckError() error {
	if a.lostErr != nil {
		return a.lostErr
	}
	return context.DeadlineExceeded
}

func (a *ackLossJS) Publish(ctx context.Context, subject string, payload []byte, opts ...jetstream.PublishOpt) (*jetstream.PubAck, error) {
	if subject == a.subject && a.fired.CompareAndSwap(false, true) {
		if !a.dropBeforePublish {
			if _, err := a.JetStream.Publish(ctx, subject, payload, opts...); err != nil {
				return nil, err
			}
		}
		return nil, a.hiddenAckError()
	}
	return a.JetStream.Publish(ctx, subject, payload, opts...)
}

func (a *ackLossJS) PublishMsg(ctx context.Context, msg *nats.Msg, opts ...jetstream.PublishOpt) (*jetstream.PubAck, error) {
	if msg.Subject == a.subject && a.fired.CompareAndSwap(false, true) {
		if !a.dropBeforePublish {
			if _, err := a.JetStream.PublishMsg(ctx, msg, opts...); err != nil {
				return nil, err
			}
		}
		return nil, a.hiddenAckError()
	}
	return a.JetStream.PublishMsg(ctx, msg, opts...)
}

func TestStartLostAckAndReconcile(t *testing.T) {
	all, _ := setup(t)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	const typ, id = "test", "start-lost-ack"
	recorder := &history.Recorder{}
	dropped := &ackLossJS{JetStream: all[0], subject: "wf.inv." + typ + "." + id}
	_, err := client.NewObserved(dropped, recorder).Start(ctx, typ, id, []byte(`1`))
	if !dropped.fired.Load() || !errors.Is(err, client.ErrAlreadyStarted) && !errors.Is(err, client.ErrStartUnknown) {
		t.Fatalf("lost start ack: fired=%v err=%v", dropped.fired.Load(), err)
	}
	inv, err := all[0].Stream(ctx, "WF_INV")
	if err != nil {
		t.Fatal(err)
	}
	info, err := inv.Info(ctx)
	if err != nil || info.State.Msgs != 1 {
		t.Fatalf("invocation stream=%+v err=%v", info.State, err)
	}
	run, err := all[0].Stream(ctx, "WF_RUN")
	if err != nil {
		t.Fatal(err)
	}
	info, err = run.Info(ctx)
	if err != nil || info.State.Msgs != 1 {
		t.Fatalf("run stream after lost-ack repair=%+v err=%v", info.State, err)
	}
	scan, err := reconcile.NewStartScan(all[1]).Scan(ctx, 0, 10, false)
	if err != nil || scan.Reenqueued != 1 {
		t.Fatalf("start repair=%+v err=%v", scan, err)
	}
	base := client.NewObserved(all[2], recorder)
	if _, err := base.Start(ctx, typ, id, []byte(`1`)); !errors.Is(err, client.ErrAlreadyStarted) {
		t.Fatalf("matching retry: %v", err)
	}
	if _, err := base.Start(ctx, typ, id, []byte(`2`)); !errors.Is(err, client.ErrInputMismatch) {
		t.Fatalf("mismatched retry: %v", err)
	}
	if result, err := history.CheckStarts(recorder.Snapshot(), time.Second); err != nil || result != porcupine.Ok {
		t.Fatalf("start history=%s err=%v", result, err)
	}
}

func TestJournalLostAckBranches(t *testing.T) {
	for _, dropBefore := range []bool{false, true} {
		name := "committed"
		if dropBefore {
			name = "absent"
		}
		for _, lostErr := range []error{context.DeadlineExceeded, nats.ErrConnectionReconnecting} {
			caseName := name + "/deadline"
			if errors.Is(lostErr, nats.ErrConnectionReconnecting) {
				caseName = name + "/disconnected"
			}
			t.Run(caseName, func(t *testing.T) {
				all, _ := setup(t)
				ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
				defer cancel()
				id := "journal-ack-" + name
				dropped := &ackLossJS{JetStream: all[0], subject: "wf.jrn.test." + id, dropBeforePublish: dropBefore, lostErr: lostErr}
				j := journal.New(dropped)
				entry := journal.Entry{Epoch: 0, Index: 0, Kind: journal.Started, WorkerID: "writer-a"}
				if _, err := j.Append(ctx, "test", id, entry, 0); !errors.Is(err, journal.ErrUnknown) || !dropped.fired.Load() {
					t.Fatalf("first append: fired=%v err=%v", dropped.fired.Load(), err)
				}
				stored := journal.New(all[1])
				records, tail, err := stored.Read(ctx, "test", id)
				if err != nil {
					t.Fatal(err)
				}
				if dropBefore {
					if len(records) != 0 || tail != 0 {
						t.Fatalf("absent append: records=%d tail=%d", len(records), tail)
					}
					if _, err := j.Append(ctx, "test", id, entry, 0); err != nil {
						t.Fatalf("retry absent append: %v", err)
					}
				} else {
					if len(records) != 1 || records[0].WorkerID != "writer-a" || tail == 0 {
						t.Fatalf("committed append: records=%+v tail=%d", records, tail)
					}
					if _, err := j.Append(ctx, "test", id, entry, 0); !errors.Is(err, journal.ErrStale) {
						t.Fatalf("retry committed append: %v", err)
					}
					if _, err := j.Append(ctx, "test", id, journal.Entry{Epoch: 1, Index: 1, Kind: journal.StepRequested, WorkerID: "writer-b"}, tail); err != nil {
						t.Fatalf("next append: %v", err)
					}
				}
				if records, _, err := stored.Read(ctx, "test", id); err != nil || len(records) != 1+boolInt(!dropBefore) {
					t.Fatalf("final journal: records=%d err=%v", len(records), err)
				}
			})
		}
	}
}

func TestJournalNetworkLostAckAfterCommit(t *testing.T) {
	all, cluster := setup(t)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	proxy, err := testcluster.NewClientProxy(cluster.Servers[0].ClientURL())
	if err != nil {
		t.Fatal(err)
	}
	defer proxy.Close()
	nc, err := proxy.Connect()
	if err != nil {
		t.Fatal(err)
	}
	defer nc.Close()
	proxied, err := jetstream.New(nc)
	if err != nil {
		t.Fatal(err)
	}
	const typ, id = "test", "network-lost-journal-ack"
	wrapped := &holdJournalResponseJS{JetStream: proxied, proxy: proxy, held: make(chan struct{})}
	entry := journal.Entry{Epoch: 1, Index: 0, Kind: journal.Started, WorkerID: "network-writer"}
	attempt, stopAttempt := context.WithTimeout(ctx, 2*time.Second)
	defer stopAttempt()
	result := make(chan error, 1)
	go func() { _, err := journal.New(wrapped).Append(attempt, typ, id, entry, 0); result <- err }()
	select {
	case <-wrapped.held:
	case <-ctx.Done():
		t.Fatal("publish was not reached")
	}
	reader := journal.New(all[1])
	var records []journal.Record
	for ctx.Err() == nil {
		records, _, err = reader.Read(ctx, typ, id)
		if err != nil {
			t.Fatal(err)
		}
		if len(records) == 1 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if len(records) != 1 || records[0].WorkerID != entry.WorkerID {
		t.Fatalf("server did not commit held-ack append: records=%+v", records)
	}
	proxy.Block() // drop the held server response on the actual TCP connection
	select {
	case err := <-result:
		if !errors.Is(err, journal.ErrUnknown) {
			t.Fatalf("network-lost ack returned %v", err)
		}
	case <-ctx.Done():
		t.Fatal("publish did not return after connection cut")
	}
	proxy.Heal()
	if _, err := reader.Append(ctx, typ, id, entry, 0); !errors.Is(err, journal.ErrStale) {
		t.Fatalf("retry of committed append: %v", err)
	}
	if records, _, err := reader.Read(ctx, typ, id); err != nil || len(records) != 1 {
		t.Fatalf("journal after network ack loss: records=%+v err=%v", records, err)
	}
}

func TestStartNetworkLostAckAfterCommit(t *testing.T) {
	all, cluster := setup(t)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	proxy, err := testcluster.NewClientProxy(cluster.Servers[0].ClientURL())
	if err != nil {
		t.Fatal(err)
	}
	defer proxy.Close()
	nc, err := proxy.Connect()
	if err != nil {
		t.Fatal(err)
	}
	defer nc.Close()
	proxied, err := jetstream.New(nc)
	if err != nil {
		t.Fatal(err)
	}
	const typ, id = "test", "network-lost-start-ack"
	wrapped := &holdJournalResponseJS{JetStream: proxied, proxy: proxy, held: make(chan struct{})}
	recorder := &history.Recorder{}
	attempt, stopAttempt := context.WithTimeout(ctx, 2*time.Second)
	defer stopAttempt()
	result := make(chan error, 1)
	go func() {
		_, err := client.NewObserved(wrapped, recorder).Start(attempt, typ, id, []byte(`1`))
		result <- err
	}()
	select {
	case <-wrapped.held:
	case <-ctx.Done():
		t.Fatal("start publish was not reached")
	}
	inv, err := all[1].Stream(ctx, "WF_INV")
	if err != nil {
		t.Fatal(err)
	}
	for ctx.Err() == nil {
		message, err := inv.GetLastMsgForSubject(ctx, "wf.inv."+typ+"."+id)
		if err == nil && string(message.Data) == "1" {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if ctx.Err() != nil {
		t.Fatal("server did not commit held-ack start")
	}
	proxy.Block()
	select {
	case err := <-result:
		if !errors.Is(err, client.ErrStartUnknown) {
			t.Fatalf("network-lost start ack returned %v", err)
		}
	case <-ctx.Done():
		t.Fatal("start did not return after connection cut")
	}
	proxy.Heal()
	base := client.NewObserved(all[2], recorder)
	if _, err := base.Start(ctx, typ, id, []byte(`1`)); !errors.Is(err, client.ErrAlreadyStarted) {
		t.Fatalf("matching retry: %v", err)
	}
	if _, err := base.Start(ctx, typ, id, []byte(`2`)); !errors.Is(err, client.ErrInputMismatch) {
		t.Fatalf("changed retry: %v", err)
	}
	scan, err := reconcile.NewStartScan(all[1]).Scan(ctx, 0, 10, false)
	if err != nil || scan.Reenqueued != 1 {
		t.Fatalf("missing enqueue repair=%+v err=%v", scan, err)
	}
	if result, err := history.CheckStarts(recorder.Snapshot(), time.Second); err != nil || result != porcupine.Ok {
		t.Fatalf("network lost-ack history=%s err=%v", result, err)
	}
}

func TestSignalNetworkLostAckAfterCommit(t *testing.T) {
	all, cluster := setup(t)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	const typ, id = "test", "network-lost-signal-ack"
	if _, err := client.New(all[1]).Start(ctx, typ, id, []byte(`null`)); err != nil {
		t.Fatal(err)
	}
	proxy, err := testcluster.NewClientProxy(cluster.Servers[0].ClientURL())
	if err != nil {
		t.Fatal(err)
	}
	defer proxy.Close()
	nc, err := proxy.Connect()
	if err != nil {
		t.Fatal(err)
	}
	defer nc.Close()
	proxied, err := jetstream.New(nc)
	if err != nil {
		t.Fatal(err)
	}
	wrapped := &holdJournalResponseJS{JetStream: proxied, proxy: proxy, held: make(chan struct{})}
	recorder := &history.Recorder{}
	attempt, stopAttempt := context.WithTimeout(ctx, 2*time.Second)
	defer stopAttempt()
	result := make(chan error, 1)
	go func() {
		_, err := client.NewObserved(wrapped, recorder).Signal(attempt, typ, id, "go", []byte(`42`), "same-key")
		result <- err
	}()
	select {
	case <-wrapped.held:
	case <-ctx.Done():
		t.Fatal("signal publish was not reached")
	}
	sig, err := all[1].Stream(ctx, "WF_SIG")
	if err != nil {
		t.Fatal(err)
	}
	var firstSeq uint64
	for ctx.Err() == nil {
		message, err := sig.GetLastMsgForSubject(ctx, "wf.sig."+typ+"."+id+".go")
		if err == nil && string(message.Data) == "42" {
			firstSeq = message.Sequence
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if firstSeq == 0 {
		t.Fatal("server did not commit held-ack signal")
	}
	proxy.Block()
	select {
	case err := <-result:
		if !errors.Is(err, client.ErrSignalUnknown) {
			t.Fatalf("network-lost signal ack returned %v", err)
		}
	case <-ctx.Done():
		t.Fatal("signal did not return after connection cut")
	}
	proxy.Heal()
	scan, err := reconcile.NewSignalScan(all[2]).Scan(ctx, 0, 10, false)
	if err != nil || scan.Reenqueued != 1 {
		t.Fatalf("missing signal wakeup repair=%+v err=%v", scan, err)
	}
	base := client.NewObserved(all[2], recorder)
	seq, err := base.Signal(ctx, typ, id, "go", []byte(`42`), "same-key")
	if err != nil || seq != firstSeq {
		t.Fatalf("matching signal retry seq=%d err=%v", seq, err)
	}
	if _, err := base.Signal(ctx, typ, id, "go", []byte(`43`), "same-key"); !errors.Is(err, client.ErrSignalMismatch) {
		t.Fatalf("changed signal retry: %v", err)
	}
	if result, err := history.CheckSignals(recorder.Snapshot(), time.Second); err != nil || result != porcupine.Ok {
		t.Fatalf("network lost-ack history=%s err=%v", result, err)
	}
}

func boolInt(value bool) int {
	if value {
		return 1
	}
	return 0
}

func TestSignalLostAckRetry(t *testing.T) {
	all, _ := setup(t)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	const typ, id = "test", "signal-lost-ack"
	if _, err := client.New(all[0]).Start(ctx, typ, id, []byte(`null`)); err != nil {
		t.Fatal(err)
	}
	recorder := &history.Recorder{}
	dropped := &ackLossJS{JetStream: all[0], subject: "wf.sig." + typ + "." + id + ".go"}
	if _, err := client.NewObserved(dropped, recorder).Signal(ctx, typ, id, "go", []byte(`42`), "same-key"); !errors.Is(err, client.ErrSignalUnknown) {
		t.Fatalf("lost signal ack: %v", err)
	}
	first, err := all[1].Stream(ctx, "WF_SIG")
	if err != nil {
		t.Fatal(err)
	}
	info, err := first.Info(ctx)
	if err != nil || info.State.Msgs != 1 {
		t.Fatalf("signal stream=%+v err=%v", info.State, err)
	}
	base := client.NewObserved(all[2], recorder)
	seq, err := base.Signal(ctx, typ, id, "go", []byte(`42`), "same-key")
	if err != nil || seq != info.State.FirstSeq {
		t.Fatalf("matching signal retry seq=%d err=%v", seq, err)
	}
	if _, err := base.Signal(ctx, typ, id, "go", []byte(`43`), "same-key"); !errors.Is(err, client.ErrSignalMismatch) {
		t.Fatalf("mismatched signal retry: %v", err)
	}
	if result, err := history.CheckSignals(recorder.Snapshot(), time.Second); err != nil || result != porcupine.Ok {
		t.Fatalf("signal history=%s err=%v", result, err)
	}
}

// This runs the plan's 1,000 ambiguous journal-append cases against real
// three-replica JetStream, with a leader kill halfway through. The SDK
// boundary injects committed and absent publishes; a separate test tier still
// needs network faults during individual appends.
func TestThousandJournalLostAckRecoveries(t *testing.T) {
	all, cluster := setup(t)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	stream, err := all[0].Stream(ctx, "WF_JRN")
	if err != nil {
		t.Fatal(err)
	}
	info, err := stream.Info(ctx)
	if err != nil {
		t.Fatal(err)
	}
	leader := -1
	for node, server := range cluster.Servers {
		if server.Name() == info.Cluster.Leader {
			leader = node
		}
	}
	if leader < 0 {
		t.Fatalf("unknown journal leader %q", info.Cluster.Leader)
	}
	publisher := (leader + 1) % len(all)
	readerNode := (leader + 2) % len(all)
	stream, err = all[publisher].Stream(ctx, "WF_JRN")
	if err != nil {
		t.Fatal(err)
	}
	reader := journal.New(all[readerNode])
	for i := 0; i < 1000; i++ {
		if i == 500 {
			cluster.KillNode(leader)
			electionCtx, stopElection := context.WithTimeout(ctx, 15*time.Second)
			var lastLeader string
			var lastErr error
			moved := false
			for electionCtx.Err() == nil {
				attempt, done := context.WithTimeout(electionCtx, time.Second)
				current, err := stream.Info(attempt)
				done()
				lastErr = err
				if current != nil && current.Cluster != nil {
					lastLeader = current.Cluster.Leader
				}
				if err == nil && current.Cluster != nil && current.Cluster.Leader != "" && current.Cluster.Leader != cluster.Servers[leader].Name() {
					attempt, done := context.WithTimeout(electionCtx, time.Second)
					peer, peerErr := all[readerNode].Stream(attempt, "WF_JRN")
					if peerErr == nil {
						_, peerErr = peer.Info(attempt)
					}
					done()
					if peerErr == nil {
						moved = true
						break
					}
					lastErr = peerErr
				}
				time.Sleep(20 * time.Millisecond)
			}
			stopElection()
			if !moved {
				t.Fatalf("journal leader did not move: old=%s last=%s err=%v routes=%d,%d", cluster.Servers[leader].Name(), lastLeader, lastErr, cluster.Servers[publisher].NumRoutes(), cluster.Servers[readerNode].NumRoutes())
			}
		}
		id := fmt.Sprintf("ack-%04d", i)
		subject := "wf.jrn.test." + id
		committed := i%2 == 0
		fault := &ackLossJS{JetStream: all[publisher], subject: subject, dropBeforePublish: !committed}
		writer := journal.New(fault)
		entry := journal.Entry{Epoch: 1, Index: 0, Kind: journal.Started, WorkerID: id}
		if _, err := writer.Append(ctx, "test", id, entry, 0); !errors.Is(err, journal.ErrUnknown) || !fault.fired.Load() {
			t.Fatalf("case %d first append: fired=%v err=%v", i, fault.fired.Load(), err)
		}
		if !committed {
			records, _, err := reader.Read(ctx, "test", id)
			if err != nil || len(records) != 0 {
				t.Fatalf("case %d absent reread: records=%+v err=%v", i, records, err)
			}
			if _, err := writer.Append(ctx, "test", id, entry, 0); err != nil {
				t.Fatalf("case %d retry absent append: %v", i, err)
			}
		}
		var records []journal.Record
		for ctx.Err() == nil {
			var err error
			records, _, err = reader.Read(ctx, "test", id)
			if err != nil {
				t.Fatalf("case %d reread: %v", i, err)
			}
			if len(records) != 0 {
				break
			}
			time.Sleep(5 * time.Millisecond)
		}
		if len(records) != 1 || records[0].WorkerID != id || records[0].Index != 0 || records[0].Kind != journal.Started {
			t.Fatalf("case %d final journal: records=%+v context=%v", i, records, ctx.Err())
		}
		if committed {
			if _, err := writer.Append(ctx, "test", id, entry, 0); !errors.Is(err, journal.ErrStale) {
				t.Fatalf("case %d retry committed append: %v", i, err)
			}
		}
	}
	stream, err = all[readerNode].Stream(ctx, "WF_JRN")
	if err != nil {
		t.Fatal(err)
	}
	info, err = stream.Info(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if info.State.Msgs != 1000 || info.State.FirstSeq != 1 || info.State.LastSeq != 1000 {
		t.Fatalf("journal stream=%+v", info.State)
	}
	seen := make(map[string]bool, 1000)
	for seq := uint64(1); seq <= 1000; seq++ {
		message, err := stream.GetMsg(ctx, seq)
		if err != nil || seen[message.Subject] {
			t.Fatalf("sequence %d: msg=%+v err=%v", seq, message, err)
		}
		seen[message.Subject] = true
	}
}

func TestStartRepairsTransientEnqueueOnRealCluster(t *testing.T) {
	all, _ := setup(t)
	ctx, stop := context.WithTimeout(context.Background(), 20*time.Second)
	defer stop()
	for _, committed := range []bool{false, true} {
		id := fmt.Sprintf("transient-enqueue-%t", committed)
		runSubject := identity.RunSubject("test", id, provision.Partitions)
		fault := &ackLossJS{JetStream: all[0], subject: runSubject, dropBeforePublish: !committed, lostErr: jetstream.ErrNoStreamResponse}
		handle, err := client.New(fault).Start(ctx, "test", id, []byte(`null`))
		if err != nil || handle.InvSeq == 0 || !fault.fired.Load() {
			t.Fatalf("committed=%t start=%+v fired=%v err=%v", committed, handle, fault.fired.Load(), err)
		}
		retried, err := client.New(all[1]).Start(ctx, "test", id, []byte(`null`))
		if !errors.Is(err, client.ErrAlreadyStarted) || retried.InvSeq != handle.InvSeq {
			t.Fatalf("committed=%t matching retry=%+v err=%v", committed, retried, err)
		}
	}
	for i, js := range all {
		for _, name := range []string{"WF_INV", "WF_RUN"} {
			stream, err := js.Stream(ctx, name)
			if err != nil {
				t.Fatal(err)
			}
			info, err := stream.Info(ctx)
			if err != nil || info.State.Msgs != 2 {
				t.Fatalf("node=%d stream=%s counts=%+v err=%v", i, name, info, err)
			}
		}
	}
}
