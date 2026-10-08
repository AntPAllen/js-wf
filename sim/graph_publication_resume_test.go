package sim

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"js-wf/internal/graphpublication"
)

var graphReaderResumeModes = []string{"retire_resume", "renew_resume", "released", "expired_uncollected", "expired_collected", "unknown_root", "fingerprint_changed", "foreign_destination", "collector_after_resume"}

func runGraphReaderResume(seed int64, replay *Trace) (trace Trace, runErr error) {
	s := NewScheduler(seed)
	if replay != nil {
		var err error
		s, err = ReplayScheduler(*replay)
		if err != nil {
			return Trace{}, err
		}
	}
	if err := s.SetWorkload("graph_reader_resume"); err != nil {
		return Trace{}, err
	}
	defer func() { trace = s.Trace() }()
	mode, err := s.Choose(graphReaderResumeModes)
	if err != nil {
		return trace, err
	}
	m := NewGraphPublicationTransport(s)
	p := m.Protocol()
	ctx := context.Background()
	now := func() time.Time { return time.UnixMilli(s.NowMillis()).UTC() }
	start := now()
	prepared, err := p.PrepareAppend(ctx, "owner", 0, []byte("checkpoint-value"), [][]byte{[]byte("checkpoint-payload")}, start.Add(time.Second))
	if err != nil {
		return trace, err
	}
	root, err := p.Commit(ctx, prepared)
	if err != nil {
		return trace, err
	}
	reader, root, err := p.AcquireReader(ctx, "owner", root.Head, start.Add(3*time.Second))
	if err != nil {
		return trace, err
	}
	want, err := p.ReadRetained(ctx, reader, 0, now())
	if err != nil {
		return trace, err
	}
	checkpoint, err := reader.Checkpoint()
	if err != nil {
		return trace, err
	}
	if err = p.RetireLive(ctx, "owner", root.Head); err != nil {
		return trace, err
	}
	root, err = m.ReadRoot(ctx, "owner")
	if err != nil {
		return trace, err
	}
	switch mode {
	case "renew_resume":
		if _, err = p.RenewReader(ctx, reader, root.Head, start.Add(6*time.Second)); err != nil {
			return trace, err
		}
		if err = s.AdvanceMillis(4000); err != nil {
			return trace, err
		}
	case "released":
		if _, err = p.ReleaseReader(ctx, reader, root.Head); err != nil {
			return trace, err
		}
	case "expired_uncollected", "expired_collected":
		if err = s.AdvanceMillis(4000); err != nil {
			return trace, err
		}
		if mode == "expired_collected" {
			if _, err = p.Sweep(ctx, now()); err != nil {
				return trace, err
			}
		}
	case "fingerprint_changed":
		encoded := []byte(fmt.Sprintf(`"SnapshotSHA256":"%s"`, digest(mustGraphEncoding(reader))))
		checkpoint = bytes.Replace(checkpoint, encoded, []byte(`"SnapshotSHA256":"`+strings.Repeat("a", 64)+`"`), 1)
	case "foreign_destination":
		checkpoint = bytes.Replace(checkpoint, []byte(`"Destination":"owner"`), []byte(`"Destination":"foreign"`), 1)
	case "unknown_root":
		if err = m.QueueFault("read_root", DropBeforeCommit); err != nil {
			return trace, err
		}
	}
	// A fresh coordinator receives checkpoint bytes; the original Reader is gone.
	reader = graphpublication.Reader{}
	p = m.Protocol()
	recovered, observed, err := p.ResumeReader(ctx, checkpoint, now())
	if mode == "unknown_root" {
		if !errors.Is(err, ErrTransportLost) {
			return trace, fmt.Errorf("unknown root recovered: %v", err)
		}
		recovered, observed, err = p.ResumeReader(ctx, checkpoint, now())
	}
	if mode == "released" || mode == "expired_uncollected" || mode == "expired_collected" || mode == "fingerprint_changed" || mode == "foreign_destination" {
		if !errors.Is(err, graphpublication.ErrRevoked) {
			return trace, fmt.Errorf("revoked checkpoint recovered: %v", err)
		}
		if mode == "released" || mode == "expired_uncollected" {
			objects, e := m.Objects(ctx)
			if e != nil || len(objects) == 0 {
				return trace, fmt.Errorf("rejection lacks physical-byte control")
			}
		}
	} else {
		if err != nil || observed.Graph.Count != 0 || observed.Schema != graphpublication.RetentionSchema {
			return trace, fmt.Errorf("canonical recovery failed: %v", err)
		}
		if mode == "collector_after_resume" {
			m.PauseBefore("get", func() error {
				if e := s.AdvanceMillis(4000); e != nil {
					return e
				}
				_, e := p.Sweep(ctx, now())
				return e
			})
			if _, err = p.ReadRetained(ctx, recovered, 0, now()); err == nil {
				return trace, fmt.Errorf("expired in-flight read succeeded")
			}
			if _, _, err = p.ResumeReader(ctx, checkpoint, now()); !errors.Is(err, graphpublication.ErrRevoked) {
				return trace, fmt.Errorf("collected checkpoint resurrected: %v", err)
			}
		} else {
			got, e := p.ReadRetained(ctx, recovered, 0, now())
			if e != nil || !reflect.DeepEqual(got, want) {
				return trace, fmt.Errorf("restored snapshot differs: %v", e)
			}
			payload, e := m.Get(ctx, got.Blobs[0], 100)
			if e != nil || string(payload) != "checkpoint-payload" {
				return trace, fmt.Errorf("restored payload differs: %v", e)
			}
			// Returned metadata and snapshots cannot alter the retained authority.
			observed.Readers[0].Graph.Frontier[0].Link.Hash = "changed-copy"
			snapshot := recovered.Snapshot()
			snapshot.Frontier[0].Link.Hash = "changed-snapshot"
			if _, _, e = p.ResumeReader(ctx, checkpoint, now()); e != nil {
				return trace, e
			}
		}
	}
	if err = m.CheckReferences(); err != nil {
		return trace, err
	}
	if err = s.AdvanceMillis(10000); err != nil {
		return trace, err
	}
	root, err = m.ReadRoot(ctx, "owner")
	if err != nil {
		return trace, err
	}
	if _, err = p.ExpireReaders(ctx, "owner", root.Head, now()); err != nil {
		return trace, err
	}
	if _, err = p.Sweep(ctx, now()); err != nil {
		return trace, err
	}
	objects, err := m.Objects(ctx)
	if err != nil || len(objects) != 0 {
		return trace, fmt.Errorf("resume final drain failed: %v", err)
	}
	root, err = m.ReadRoot(ctx, "owner")
	if err != nil || root.Schema != graphpublication.RetentionSchema || root.Graph.Count != 0 || len(root.Readers) != 0 {
		return trace, fmt.Errorf("resume final root failed: %v", err)
	}
	if err = m.CheckReferences(); err != nil {
		return trace, err
	}
	s.RecordTransport(TransportEvent{Operation: "check_graph_reader_resume", Outcome: mode})
	if err = s.Finish(); err != nil {
		return trace, err
	}
	return trace, nil
}

func mustGraphEncoding(reader graphpublication.Reader) []byte {
	data, err := reader.Snapshot().Encode()
	if err != nil {
		panic(err)
	}
	return data
}

func TestSeededGraphReaderResumeReplay(t *testing.T) {
	observed := map[string]int{}
	fail := func(seed int64, trace Trace, cause error) {
		path := os.Getenv("FAULT_TRACE_OUT")
		if path == "" {
			dir, err := os.MkdirTemp("", "js-wf-graph-reader-resume-failure-")
			if err != nil {
				t.Fatal(err)
			}
			path = filepath.Join(dir, "trace.json")
		}
		if err := trace.Save(path); err != nil {
			t.Fatal(err)
		}
		t.Fatalf("FAULT_SEED=%d FAULT_TRACE=%s: %v", seed, path, cause)
	}
	for seed := range seededSchedules(t) {
		generated, err := runGraphReaderResume(seed, nil)
		if err != nil {
			fail(seed, generated, err)
		}
		mode := generated.Decisions[0].Chosen
		observed[mode]++
		replayed, err := runGraphReaderResume(seed, &generated)
		if err != nil || !reflect.DeepEqual(generated, replayed) {
			fail(seed, generated, fmt.Errorf("reader recovery replay differs: %v", err))
		}
		if dir := os.Getenv("SIM_GRAPH_READER_RESUME_ROOT"); dir != "" && observed[mode] == 1 {
			if err = generated.Save(filepath.Join(dir, mode+".json")); err != nil {
				t.Fatal(err)
			}
		}
	}
	if len(observed) != len(graphReaderResumeModes) {
		t.Fatalf("reader recovery coverage=%v", observed)
	}
	t.Logf("reader recovery: modes=%v; exact replay, canonical identity and complete drain", observed)
}
