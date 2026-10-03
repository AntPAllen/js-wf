package retention

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"testing"
	"time"

	"js-wf/provision"
	"js-wf/testcluster"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// Key absence alone does not establish physical retention cleanup: KV Delete
// writes a new retained DEL revision in a History=1 bucket.
func TestTombstoneScanDrainsPhysicalDeleteMarkers(t *testing.T) {
	cluster, err := testcluster.Start(t.TempDir(), 1)
	if err != nil {
		t.Fatal(err)
	}
	defer cluster.Close()
	js, err := jetstream.New(cluster.Clients[0])
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if err := provision.Ensure(ctx, js, 1); err != nil {
		t.Fatal(err)
	}
	state, err := js.KeyValue(ctx, "WF_STATE")
	if err != nil {
		t.Fatal(err)
	}
	stream, err := js.Stream(ctx, "KV_WF_STATE")
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	value, err := json.Marshal(Tombstone{Tombstone: true, InvSeq: 1, PurgedAt: now.Add(-time.Hour), ExpiresAt: now.Add(-time.Minute)})
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 32; i++ {
		if _, err := state.Put(ctx, fmt.Sprintf("test.expired-%d", i), value); err != nil {
			t.Fatal(err)
		}
	}
	const protected = `{"inv_seq":99,"result":true}`
	if _, err := state.Put(ctx, "test.protected", []byte(protected)); err != nil {
		t.Fatal(err)
	}
	scan := NewTombstoneScan(js)
	// The first pass replaces tombstones with DEL markers; the next encounters
	// those newer stream revisions. Three passes allow wrap at a fixed page limit.
	for pass := 0; pass < 3; pass++ {
		cursor := uint64(1)
		for page := 0; page < 100; page++ {
			result, err := scan.Scan(ctx, cursor, 7, now, false)
			if err != nil {
				t.Fatal(err)
			}
			cursor = result.NextSequence
			if cursor == 1 {
				break
			}
			if page == 99 {
				t.Fatal("scan did not wrap")
			}
		}
	}
	for i := 0; i < 32; i++ {
		if _, err := state.Get(ctx, fmt.Sprintf("test.expired-%d", i)); !errors.Is(err, jetstream.ErrKeyNotFound) {
			t.Fatalf("expired key: %v", err)
		}
	}
	entry, err := state.Get(ctx, "test.protected")
	if err != nil || string(entry.Value()) != protected {
		t.Fatalf("protected result changed: %v", err)
	}
	info, err := stream.Info(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("physical_messages=%d physical_bytes=%d subjects=%d last_seq=%d", info.State.Msgs, info.State.Bytes, info.State.NumSubjects, info.State.LastSeq)
	if info.State.Msgs != 1 || info.State.NumSubjects != 1 {
		t.Fatalf("expired delete markers retained: %+v", info.State)
	}
	// Once the marker is gone, KV Create must also accept a reused identity.
	revision, err := state.Create(ctx, "test.expired-0", []byte(`{"inv_seq":2,"result":true}`))
	if err != nil || revision <= info.State.LastSeq {
		t.Fatalf("reuse after physical purge: revision=%d error=%v", revision, err)
	}
}

type markerReusePort struct {
	TombstoneScanPort
	state      jetstream.KeyValue
	key, fault string
	revision   uint64
	attempts   int
}
type markerReuseSession struct {
	TombstoneScanSession
	owner *markerReusePort
}

func (p *markerReusePort) Open(ctx context.Context) (TombstoneScanSession, error) {
	s, err := p.TombstoneScanPort.Open(ctx)
	if err != nil {
		return nil, err
	}
	return markerReuseSession{s, p}, nil
}
func (s markerReuseSession) DeleteStateMarker(ctx context.Context, key string, seq uint64) error {
	p := s.owner
	if key != p.key || seq != p.revision {
		return fmt.Errorf("unexpected marker purge:%s/%d", key, seq)
	}
	p.attempts++
	if p.attempts == 1 {
		// Advance this subject after its marker was observed but before its
		// purge. History=1 has already removed the obsolete marker here.
		if _, err := p.state.Create(ctx, key, []byte(`{"inv_seq":2,"result":true}`)); err != nil {
			return err
		}
		if p.fault == "drop" {
			return nats.ErrTimeout
		}
	}
	if err := s.TombstoneScanSession.DeleteStateMarker(ctx, key, seq); err != nil {
		return err
	}
	if p.attempts == 1 && p.fault == "lost_ack" {
		return nats.ErrTimeout
	}
	return nil
}

func TestTombstoneMarkerPurgeProtectsConcurrentReuse(t *testing.T) {
	cluster, err := testcluster.Start(t.TempDir(), 3)
	if err != nil {
		t.Fatal(err)
	}
	defer cluster.Close()
	js, err := jetstream.New(cluster.Clients[0])
	if err != nil {
		t.Fatal(err)
	}
	ctx, stop := context.WithTimeout(context.Background(), 30*time.Second)
	defer stop()
	for ctx.Err() == nil {
		attempt, done := context.WithTimeout(ctx, 2*time.Second)
		_, err = js.AccountInfo(attempt)
		done()
		if err == nil {
			attempt, done = context.WithTimeout(ctx, 2*time.Second)
			err = provision.Ensure(attempt, js, 3)
			done()
		}
		if err == nil {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if err != nil {
		t.Fatal(err)
	}
	state, err := js.KeyValue(ctx, "WF_STATE")
	if err != nil {
		t.Fatal(err)
	}
	stream, err := js.Stream(ctx, "KV_WF_STATE")
	if err != nil {
		t.Fatal(err)
	}
	for _, op := range []string{"DEL", "PURGE"} {
		for _, fault := range []string{"ack", "drop", "lost_ack"} {
			t.Run(op+"_"+fault, func(t *testing.T) {
				key := "test.marker-" + op + "-" + fault
				if _, err := state.Create(ctx, key, []byte(`{"inv_seq":1,"result":true}`)); err != nil {
					t.Fatal(err)
				}
				if op == "DEL" {
					err = state.Delete(ctx, key)
				} else {
					err = state.Purge(ctx, key)
				}
				if err != nil {
					t.Fatal(err)
				}
				marker, err := stream.GetLastMsgForSubject(ctx, "$KV.WF_STATE."+key)
				if err != nil {
					t.Fatal(err)
				}
				port := &markerReusePort{TombstoneScanPort: NewTombstoneScanPort(js), state: state, key: key, fault: fault, revision: marker.Sequence}
				scan := NewTombstoneScanWithPort(port)
				_, err = scan.Scan(ctx, marker.Sequence, 1, time.Now(), false)
				if fault == "ack" {
					if err != nil {
						t.Fatal(err)
					}
				} else if !errors.Is(err, nats.ErrTimeout) {
					t.Fatalf("expected hidden/drop ack:%v", err)
				}
				if _, err := scan.Scan(ctx, marker.Sequence, 2, time.Now(), false); err != nil {
					t.Fatal(err)
				}
				entry, err := state.Get(ctx, key)
				if err != nil || string(entry.Value()) != `{"inv_seq":2,"result":true}` {
					t.Fatalf("new generation lost:%v", err)
				}
				if _, err := stream.GetMsg(ctx, marker.Sequence); !errors.Is(err, jetstream.ErrMsgNotFound) {
					t.Fatalf("obsolete physical marker remains:%v", err)
				}
				t.Logf("op=%s fault=%s marker_seq=%d newer_seq=%d attempts=%d", op, fault, marker.Sequence, entry.Revision(), port.attempts)
			})
		}
	}
	info, err := stream.Info(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if info.State.Msgs != 6 || info.State.NumSubjects != 6 {
		t.Fatalf("unexpected physical state:%+v", info.State)
	}
}
