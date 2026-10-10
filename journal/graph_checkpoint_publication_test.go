package journal_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"js-wf/internal/graphpublication"
	"js-wf/journal"
	"js-wf/sim"
)

type checkpointPublicationPort struct {
	*checkpointCostPort
	mode                             string
	armed, failRead                  bool
	pointerAttempts, releaseAttempts int
	race                             func() error
}

func (p *checkpointPublicationPort) ReadRoot(ctx context.Context, key string) (graphpublication.Root, error) {
	if p.failRead {
		p.failRead = false
		return graphpublication.Root{}, context.DeadlineExceeded
	}
	return p.GraphPublicationTransport.ReadRoot(ctx, key)
}

func (p *checkpointPublicationPort) CASRoot(ctx context.Context, key string, head uint64, root graphpublication.Root) (graphpublication.Root, error) {
	var app struct {
		Checkpoint json.RawMessage `json:"checkpoint"`
	}
	if err := json.Unmarshal(root.Application, &app); err != nil {
		return graphpublication.Root{}, err
	}
	phase := ""
	if len(root.Readers) == 0 {
		before, err := p.GraphPublicationTransport.ReadRoot(ctx, key)
		if err != nil {
			return graphpublication.Root{}, err
		}
		if len(before.Readers) > 0 {
			phase = "release"
			p.releaseAttempts++
		} else if len(app.Checkpoint) > 0 && !bytes.Equal(before.Application, root.Application) {
			phase = "pointer"
			p.pointerAttempts++
		}
	}
	if phase == "pointer" && p.race != nil {
		race := p.race
		p.race = nil
		if err := race(); err != nil {
			return graphpublication.Root{}, err
		}
	}
	injected := p.armed && strings.HasPrefix(p.mode, phase+"-") && phase != ""
	if injected {
		p.armed = false
		fault := sim.LoseAckAfterCommit
		if strings.HasSuffix(p.mode, "before") {
			fault = sim.DropBeforeCommit
		}
		if err := p.QueueFault("cas_root", fault); err != nil {
			return graphpublication.Root{}, err
		}
	}
	ack, err := p.checkpointCostPort.CASRoot(ctx, key, head, root)
	if injected && err != nil && strings.HasSuffix(p.mode, "unconfirmed") {
		p.failRead = true
	}
	return ack, err
}

func publicationRoot(t *testing.T, p *checkpointPublicationPort) (graphpublication.Root, bool) {
	t.Helper()
	keys, err := p.RootKeys(context.Background())
	if err != nil || len(keys) != 1 {
		t.Fatal(keys, err)
	}
	root, err := p.GraphPublicationTransport.ReadRoot(context.Background(), keys[0])
	if err != nil {
		t.Fatal(err)
	}
	var app struct {
		Checkpoint json.RawMessage `json:"checkpoint"`
	}
	if err = json.Unmarshal(root.Application, &app); err != nil {
		t.Fatal(err)
	}
	return root, len(app.Checkpoint) > 0
}

func TestGraphCheckpointPublicationBatchesAndFinalization(t *testing.T) {
	for _, encoding := range []journal.Encoding{journal.JSON, journal.ProtobufV1} {
		for _, mode := range []string{"normal", "paused", "renewing", "tail-change", "tail-race", "wrong-runtime", "wrong-tail", "closed", "expired", "release-before", "release-confirmed", "release-unconfirmed", "pointer-before", "pointer-confirmed", "pointer-unconfirmed"} {
			t.Run(string(encoding)+"/"+mode, func(t *testing.T) {
				ctx := context.Background()
				var port *checkpointPublicationPort
				store, base, now, runtime := checkpointScanFixture(t, encoding, []byte(`{"result":7}`), 16, checkpointScanFixtureOptions{indexed: true, wrap: func(p *checkpointCostPort) graphpublication.Port {
					port = &checkpointPublicationPort{checkpointCostPort: p, mode: mode}
					return port
				}})
				tail := runtime.Sequence + 1
				requested, requestedTail := runtime, tail
				if mode == "wrong-runtime" {
					requested.Stage = "foreign"
				}
				if mode == "wrong-tail" {
					requestedTail++
				}
				publication, err := store.BeginCheckpointPublication(ctx, "flow", "scan", requested, requestedTail)
				if err != nil {
					t.Fatal(err)
				}
				defer publication.Close(ctx)
				port.pointerAttempts, port.releaseAttempts = 0, 0
				base.entryReads = 0
				if mode == "renewing" {
					base.clock = now
				}
				done, err := publication.Advance(ctx, 3)
				if err != nil || done || publication.NextIndex() != 3 || base.entryReads != 3 {
					t.Fatal("partial publication", done, err, publication.NextIndex(), base.entryReads)
				}
				root, published := publicationRoot(t, port)
				if published || port.pointerAttempts != 0 || len(root.Readers) != 1 {
					t.Fatal("partial scan changed pointer or released reader", root, port.pointerAttempts)
				}
				attempts := base.entryReads
				if mode == "paused" {
					base.entryReads, base.maxEntryReads = 0, 5
					done, err = publication.Advance(ctx, 9)
					attempts += base.entryReads
					if !errors.Is(err, context.DeadlineExceeded) || done || publication.NextIndex() != 8 {
						t.Fatal("lost progress", done, err, publication.NextIndex())
					}
					base.maxEntryReads = 0
					cancelled, cancel := context.WithCancel(ctx)
					cancel()
					if done, err = publication.Advance(cancelled, 4); done || !errors.Is(err, context.Canceled) || publication.NextIndex() != 8 {
						t.Fatal(done, err)
					}
				}
				appendTail := func() error {
					_, err := store.Append(ctx, "flow", "scan", runtime.InvSeq, journal.Entry{Kind: journal.Suspended, Index: 36, Epoch: 3, Payload: []byte(`{"waiting_on":"changed"}`)}, tail, nil, nil)
					return err
				}
				if mode == "tail-change" {
					if err = appendTail(); err != nil {
						t.Fatal(err)
					}
				}
				if mode == "tail-race" {
					port.race = appendTail
				}
				if mode == "closed" {
					if err = publication.Close(ctx); err != nil {
						t.Fatal(err)
					}
				}
				if mode == "expired" {
					*now = now.Add(5 * time.Second)
				}
				port.armed = true
				before, _ := publicationRoot(t, port)
				for !done {
					start := publication.NextIndex()
					base.entryReads = 0
					done, err = publication.Advance(ctx, 4)
					attempts += base.entryReads
					if publication.NextIndex()-start > 4 || base.entryReads > int(publication.NextIndex()-start)+1 {
						t.Fatal("unbounded batch", start, publication.NextIndex(), base.entryReads)
					}
					_, published = publicationRoot(t, port)
					if err != nil {
						break
					}
					if !done && published {
						t.Fatal("partial scan published pointer")
					}
				}
				root, published = publicationRoot(t, port)
				wantSuccess := mode == "normal" || mode == "paused" || mode == "renewing" || mode == "release-confirmed" || mode == "pointer-confirmed"
				if wantSuccess {
					if err != nil || !done || !published || len(root.Readers) != 0 {
						t.Fatal("complete scan failed", done, err, published, len(root.Readers))
					}
					wantReads := 37
					if mode == "paused" {
						wantReads++
					}
					if attempts != wantReads {
						t.Fatal("verified prefix rescanned", attempts, wantReads)
					}
					if mode == "renewing" && (base.renewals == 0 || now.Sub(time.Unix(1000, 0)) != 37*time.Second) {
						t.Fatal("reader not renewed", base.renewals, *now)
					}
					base.clock = nil
					if again, e := publication.Advance(ctx, 1); !again || e != nil {
						t.Fatal(again, e)
					}
					if err = store.ConfirmCheckpoint(ctx, "flow", "scan", runtime, tail); err != nil {
						t.Fatal("published pointer does not verify", err)
					}
				} else {
					if err == nil || done || (published && mode != "pointer-unconfirmed") {
						t.Fatal("unverified publication admitted", done, err, published)
					}
					if strings.HasPrefix(mode, "release-") && (port.pointerAttempts != 0 || !errors.Is(err, journal.ErrUnknown)) {
						t.Fatal("uncertain release authorized pointer", port.pointerAttempts, err)
					}
					if mode == "closed" || mode == "expired" {
						if attempts != 3 || !reflect.DeepEqual(before, root) {
							t.Fatal("inactive operation read or changed authority", attempts, before, root)
						}
					}
					if again, e := publication.Advance(ctx, 4); again || e == nil {
						t.Fatal("failed finalization reused", again, e)
					}
					port.armed = false
					_ = publication.Close(ctx)
					root, _ = publicationRoot(t, port)
					if len(root.Readers) != 0 {
						t.Fatal("abandoned reader not removed", root.Readers)
					}
					if strings.HasPrefix(mode, "release-") || strings.HasPrefix(mode, "pointer-") {
						if err = store.PublishCheckpoint(ctx, "flow", "scan", runtime, tail); err != nil {
							t.Fatal("fresh retry did not reconcile", err)
						}
						root, published = publicationRoot(t, port)
						if !published || len(root.Readers) != 0 {
							t.Fatal("fresh retry failed", published, root.Readers)
						}
					}
				}
				t.Logf("CHECKPOINT_PUBLICATION mode=%s next=%d entry_read_attempts=%d renewals=%d pointer_attempts=%d release_attempts=%d", mode, publication.NextIndex(), attempts, base.renewals, port.pointerAttempts, port.releaseAttempts)
			})
		}
	}
}
