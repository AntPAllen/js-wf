package graphpublication

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"
)

func TestGraphReaderCheckpointRecovery(t *testing.T) {
	m, p, root, reader := readerFixture(t)
	checkpoint, err := reader.Checkpoint()
	if err != nil {
		t.Fatal(err)
	}
	want := reader.Snapshot()
	if err = p.RetireLive(ctx, "history", root.Head); err != nil {
		t.Fatal(err)
	}
	reader = Reader{}
	recovered, observed, err := p.ResumeReader(ctx, checkpoint, epoch.Add(time.Hour))
	if err != nil || observed.Head != root.Head+1 || !reflect.DeepEqual(recovered.Snapshot(), want) {
		t.Fatal(recovered, observed, err)
	}
	if m.roots["history"].Head != observed.Head {
		t.Fatal("resumption mutated head")
	}
	record, err := p.ReadRetained(ctx, recovered, 0, epoch.Add(time.Hour))
	if err != nil || string(record.Data) != "old" {
		t.Fatal(record, err)
	}
	observed, err = p.RenewReader(ctx, recovered, observed.Head, epoch.Add(3*time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	// The checkpoint contains no expiry/head. A witnessed renewal supplies them.
	recovered, observed, err = p.ResumeReader(ctx, checkpoint, epoch.Add(2*time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	if _, err = p.ReleaseReader(ctx, recovered, observed.Head); err != nil {
		t.Fatal(err)
	}
	if _, _, err = p.ResumeReader(ctx, checkpoint, epoch.Add(2*time.Hour)); !errors.Is(err, ErrRevoked) {
		t.Fatal(err)
	}
	if len(m.objects) == 0 {
		t.Fatal("revocation checked only after byte deletion")
	}
}

func TestGraphReaderCheckpointRejectsUnknownAndRevoked(t *testing.T) {
	for _, reason := range []string{"expired-uncollected", "expired-collected", "changed-snapshot", "foreign-destination", "missing-ID", "replacement-same-graph", "uncertain-root", "cancelled"} {
		t.Run(reason, func(t *testing.T) {
			m, p, root, reader := readerFixture(t)
			checkpoint, err := reader.Checkpoint()
			if err != nil {
				t.Fatal(err)
			}
			now := epoch.Add(time.Hour)
			wanted := ErrRevoked
			var v readerCheckpoint
			if err = json.Unmarshal(checkpoint, &v); err != nil {
				t.Fatal(err)
			}
			switch reason {
			case "expired-uncollected":
				now = epoch.Add(2 * time.Hour)
			case "expired-collected":
				if err = p.RetireLive(ctx, "history", root.Head); err != nil {
					t.Fatal(err)
				}
				if _, err = p.Sweep(ctx, epoch.Add(2*time.Hour)); err != nil {
					t.Fatal(err)
				}
			case "changed-snapshot":
				v.SnapshotSHA256 = key([]byte("other"))
			case "foreign-destination":
				v.Destination = "elsewhere"
			case "missing-ID":
				v.ID = "missing"
			case "replacement-same-graph":
				released, e := p.ReleaseReader(ctx, reader, root.Head)
				if e != nil {
					t.Fatal(e)
				}
				replacement, _, e := p.AcquireReader(ctx, "history", released.Head, epoch.Add(2*time.Hour))
				if e != nil || !reflect.DeepEqual(replacement.Snapshot(), reader.Snapshot()) {
					t.Fatal(e)
				}
			case "uncertain-root":
				m.readRootHook = func(string) error { return lostReply }
				wanted = lostReply
			case "cancelled":
				wanted = context.Canceled
			}
			checkpoint, err = json.Marshal(v)
			if err != nil {
				t.Fatal(err)
			}
			before, _ := json.Marshal(m.roots)
			call := ctx
			if reason == "cancelled" {
				var cancel context.CancelFunc
				call, cancel = context.WithCancel(ctx)
				cancel()
			}
			resumed, _, err := p.ResumeReader(call, checkpoint, now)
			if !errors.Is(err, wanted) || resumed.id != "" {
				t.Fatal(resumed, err)
			}
			after, _ := json.Marshal(m.roots)
			if string(before) != string(after) {
				t.Fatal("invalid recovery changed authority")
			}
		})
	}
}

func TestGraphReaderCheckpointCanonicalBounds(t *testing.T) {
	m, p, _, reader := readerFixture(t)
	data, err := reader.Checkpoint()
	if err != nil {
		t.Fatal(err)
	}
	var v readerCheckpoint
	if err = json.Unmarshal(data, &v); err != nil {
		t.Fatal(err)
	}
	invalid := [][]byte{
		nil, []byte(strings.Repeat(" ", MaxReaderCheckpointBytes+1)), append(append([]byte{}, data...), []byte(" {}")...),
		[]byte(" " + string(data)), []byte(strings.Replace(string(data), `"ID":`, `"id":`, 1)),
		[]byte(strings.Replace(string(data), `"ID":`, `"ID":"duplicate","ID":`, 1)),
		[]byte(strings.TrimSuffix(string(data), "}") + `,"Expiry":"2999-01-01T00:00:00Z"}`),
	}
	for _, change := range []func(*readerCheckpoint){
		func(v *readerCheckpoint) { v.Schema = "legacy" }, func(v *readerCheckpoint) { v.ID = "" },
		func(v *readerCheckpoint) { v.Destination = "" }, func(v *readerCheckpoint) { v.Destination = strings.Repeat("a", 257) },
		func(v *readerCheckpoint) { v.SnapshotSHA256 = strings.Repeat("A", 64) },
	} {
		bad := v
		change(&bad)
		encoded, _ := json.Marshal(bad)
		invalid = append(invalid, encoded)
	}
	reads := 0
	m.readRootHook = func(string) error { reads++; return nil }
	for _, bad := range invalid {
		if r, _, err := p.ResumeReader(ctx, bad, epoch); err == nil || r.id != "" {
			t.Fatal("malformed checkpoint accepted", string(bad))
		}
	}
	if reads != 0 {
		t.Fatal("malformed checkpoint reached authority", reads)
	}
	if _, err = (Reader{}).Checkpoint(); err == nil {
		t.Fatal("zero reader serialized")
	}
	// The full admitted destination byte length fits even with maximal JSON
	// escaping. The bound is on encoded bytes, not an ASCII-only identity.
	m.readRootHook = nil
	destination := strings.Repeat("\x00", 256)
	reader, _, err = p.AcquireReader(ctx, destination, 0, epoch.Add(time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := reader.Checkpoint()
	if err != nil || len(encoded) > MaxReaderCheckpointBytes {
		t.Fatal(len(encoded), err)
	}
	if _, _, err = p.ResumeReader(ctx, encoded, epoch); err != nil {
		t.Fatal(err)
	}
}
