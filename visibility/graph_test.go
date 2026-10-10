package visibility

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	"js-wf/client"
	"js-wf/internal/graphpublication"
	"js-wf/internal/retainedgraph"
	"js-wf/journal"
	"js-wf/sim"
	"js-wf/wf"
)

type graphViewKV struct {
	jetstream.KeyValue
	rows map[string][]byte
}
type graphViewEntry struct {
	jetstream.KeyValueEntry
	data []byte
}

func (e graphViewEntry) Value() []byte { return append([]byte(nil), e.data...) }
func (k *graphViewKV) Get(_ context.Context, key string) (jetstream.KeyValueEntry, error) {
	data, ok := k.rows[key]
	if !ok {
		return nil, jetstream.ErrKeyNotFound
	}
	return graphViewEntry{data: data}, nil
}
func (k *graphViewKV) Put(_ context.Context, key string, data []byte) (uint64, error) {
	k.rows[key] = append([]byte(nil), data...)
	return 1, nil
}
func (k *graphViewKV) Delete(_ context.Context, key string, _ ...jetstream.KVDeleteOpt) error {
	delete(k.rows, key)
	return nil
}
func (k *graphViewKV) Keys(context.Context, ...jetstream.WatchOpt) ([]string, error) {
	keys := []string{}
	for key := range k.rows {
		keys = append(keys, key)
	}
	return keys, nil
}

type graphViewInput struct {
	jetstream.Stream
	transport *sim.WorkerTransport
	forged    bool
}

func (s graphViewInput) GetLastMsgForSubject(ctx context.Context, subject string) (*jetstream.RawStreamMsg, error) {
	m, e := s.transport.LastInvocation(ctx, subject)
	if e == nil && s.forged {
		m.Header.Set(journal.GraphStartTokenHeader, "forged")
	}
	return m, e
}

type graphViewPublication struct {
	*sim.GraphPublicationTransport
	pinWrites, bodyReads int
	hash                 string
}

func (p *graphViewPublication) CASRoot(ctx context.Context, key string, head uint64, root graphpublication.Root) (graphpublication.Root, error) {
	if len(root.Readers) > 0 {
		p.pinWrites++
	}
	return p.GraphPublicationTransport.CASRoot(ctx, key, head, root)
}
func (p *graphViewPublication) Get(ctx context.Context, link retainedgraph.Link, limit int) ([]byte, error) {
	if link.Hash == p.hash {
		p.bodyReads++
	}
	return p.GraphPublicationTransport.Get(ctx, link, limit)
}

func TestGraphVisibilityCanonicalRowsAndUncertainty(t *testing.T) {
	testGraphVisibilityCanonicalRowsAndUncertainty(t, false)
}
func TestGraphPostgresVisibilityCanonicalRowsAndUncertainty(t *testing.T) {
	if os.Getenv("WF_TEST_POSTGRES_DSN") == "" {
		t.Skip("set WF_TEST_POSTGRES_DSN")
	}
	testGraphVisibilityCanonicalRowsAndUncertainty(t, true)
}
func testGraphVisibilityCanonicalRowsAndUncertainty(t *testing.T, postgres bool) {
	for _, mode := range []string{"ordinary", "catalog-unknown", "source-forged", "lease-held", "lease-unknown", "completed-lease-held", "failed-lease-held", "completed-lease-held-history-unknown", "retired"} {
		t.Run(mode, func(t *testing.T) {
			ctx := context.Background()
			schedule := sim.NewScheduler(1)
			model := sim.NewGraphPublicationTransport(schedule)
			port := &graphViewPublication{GraphPublicationTransport: model}
			protocol := model.Protocol()
			protocol.Port = port
			store, e := journal.NewGraphStore(journal.GraphConfig{Protocol: protocol, CanonicalStarts: true, CanonicalSignals: true, Now: func() time.Time { return time.Unix(1000, 0) }})
			if e != nil {
				t.Fatal(e)
			}
			transport := sim.NewWorkerTransport(schedule, 3*time.Second)
			c, e := client.NewWithSignalPorts(transport.SignalTransport, transport.SignalTransport).WithGraphJournal(store)
			if e != nil {
				t.Fatal(e)
			}
			h, e := c.Start(ctx, "visibility", "flow", []byte(`7`))
			if e != nil {
				t.Fatal(e)
			}
			state, e := store.InspectStart(ctx, h.Type, h.ID)
			if e != nil {
				t.Fatal(e)
			}
			port.hash = state.State.Start.InputSHA256
			started, _ := json.Marshal(map[string]string{"input_sha256": port.hash})
			tail, e := store.Begin(ctx, h.Type, h.ID, h.InvSeq)
			if e != nil {
				t.Fatal(e)
			}
			entries := []journal.Entry{{Kind: journal.Started, Payload: started}, {Kind: journal.StepRequested, Payload: []byte(`{"kind":"search_attributes","name":"set"}`)}, {Kind: journal.StepCompleted, Payload: []byte(`{"result":{"tenant":"one"}}`)}, {Kind: journal.Suspended, Payload: []byte(`{"waiting_on":"signal:go"}`)}}
			for i, entry := range entries {
				entry.Index = uint64(i)
				var payloads [][]byte
				if i == 0 {
					payloads = [][]byte{[]byte(`7`)}
				}
				tail, e = store.Append(ctx, h.Type, h.ID, h.InvSeq, entry, tail, payloads, nil)
				if e != nil {
					t.Fatal(e)
				}
			}
			terminalStatus := ""
			if mode == "completed-lease-held" || mode == "failed-lease-held" || mode == "completed-lease-held-history-unknown" {
				kind := journal.Completed
				outcome := wf.Outcome{InvSeq: h.InvSeq, Result: []byte(`42`)}
				terminalStatus = "completed"
				if mode == "failed-lease-held" {
					kind, terminalStatus = journal.Failed, "failed"
					outcome = wf.Outcome{InvSeq: h.InvSeq, Error: "directed failure"}
				}
				payload, _ := json.Marshal(outcome)
				tail, e = store.Append(ctx, h.Type, h.ID, h.InvSeq, journal.Entry{Index: 4, Kind: kind, Payload: payload}, tail, nil, nil)
				if e != nil {
					t.Fatal(e)
				}
			}
			view := &graphViewKV{rows: map[string][]byte{}}
			source := graphViewInput{transport: transport, forged: mode == "source-forged"}
			held := mode == "lease-held" || mode == "completed-lease-held" || mode == "failed-lease-held" || mode == "completed-lease-held-history-unknown"
			p := &Projection{graph: store, inv: source, view: view, schemaVersion: 1, graphLeaseCheck: func(context.Context, string, string) (bool, error) {
				if mode == "lease-unknown" {
					return false, context.DeadlineExceeded
				}
				return held, nil
			}}
			if postgres {
				db, err := sql.Open("pgx", os.Getenv("WF_TEST_POSTGRES_DSN"))
				if err != nil {
					t.Fatal(err)
				}
				defer db.Close()
				sink := &PostgresStore{DB: db, Namespace: t.Name()}
				if err := sink.Init(ctx); err != nil {
					t.Fatal(err)
				}
				defer db.ExecContext(context.Background(), "DROP TABLE "+sink.tableName())
				p.postgres, p.view = sink, nil
			}
			// A failed scan cannot certify absence and prune a prior view.
			prior := Row{SchemaVersion: 1, Type: h.Type, ID: h.ID, Status: "queued", InvSeq: h.InvSeq}
			if e = p.putRow(ctx, prior); e != nil {
				t.Fatal(e)
			}
			if e := p.putRow(ctx, Row{Type: "stale", ID: "prior", Status: "running"}); e != nil {
				t.Fatal(e)
			}
			if mode == "catalog-unknown" {
				if e = model.QueueFault("catalog_high_water", sim.DropBeforeCommit); e != nil {
					t.Fatal(e)
				}
			}
			if mode == "completed-lease-held-history-unknown" {
				if e = model.QueueFault("get", sim.DropBeforeCommit); e != nil {
					t.Fatal(e)
				}
			}
			port.pinWrites, port.bodyReads = 0, 0
			e = p.Rebuild(ctx)
			if mode == "completed-lease-held-history-unknown" {
				if !errors.Is(e, sim.ErrTransportLost) {
					t.Fatal("terminal summary bypassed unavailable owned history", e)
				}
				row, err := p.Get(ctx, h.Type, h.ID)
				if err != nil || row.Status != "queued" || port.pinWrites == 0 {
					t.Fatal("unknown terminal history changed prior projection", row, err, port.pinWrites)
				}
				if _, err := p.Get(ctx, "stale", "prior"); err != nil {
					t.Fatal("unknown terminal history pruned prior row", err)
				}
				return
			}
			if mode == "catalog-unknown" || mode == "source-forged" || mode == "lease-unknown" {
				if !errors.Is(e, journal.ErrUnknown) {
					t.Fatal("uncertainty certified progress", e)
				}
				row, getErr := p.Get(ctx, h.Type, h.ID)
				if getErr != nil || row.Status != "queued" || port.pinWrites != 0 {
					t.Fatal("failed scan mutated/pinned prior view", getErr, port.pinWrites)
				}
				if _, err := p.Get(ctx, "stale", "prior"); err != nil {
					t.Fatal("uncertain scan pruned unrelated prior row", err)
				}
				return
			}
			if e != nil {
				t.Fatal(e)
			}
			if _, err := p.Get(ctx, "stale", "prior"); !errors.Is(err, ErrNotFound) {
				t.Fatal("successful scan retained stale row", err)
			}
			if terminalStatus != "" {
				page, err := p.ListPage(ctx, terminalStatus, "", 1)
				if err != nil || len(page.Rows) != 1 || page.Rows[0].Status != terminalStatus || page.Rows[0].InvSeq != h.InvSeq || page.Rows[0].Attributes["tenant"] != "one" {
					t.Fatal("held lease hid canonical terminal history", page, err)
				}
				if !held || port.pinWrites == 0 || port.bodyReads != 0 {
					t.Fatal("terminal projection did not validate owned history while lease remained held", held, port.pinWrites, port.bodyReads)
				}
				if lag, err := p.Lag(ctx); err != nil || lag != 0 {
					t.Fatal("terminal lease inflated lag", lag, err)
				}
				t.Logf("terminal=%s held_lease=true owned_history_verified=true input_body_reads=0", terminalStatus)
				return
			}
			if mode == "lease-held" {
				if port.pinWrites != 0 {
					t.Fatal("active delivery was pinned")
				}
				if lag, e := p.Lag(ctx); e != nil || lag != 1 {
					t.Fatal("active delivery lag", lag, e)
				}
				held = false
				if e = p.Rebuild(ctx); e != nil {
					t.Fatal(e)
				}
			}
			row, records, e := p.Describe(ctx, h.Type, h.ID)
			if e != nil || row.Status != "suspended" || row.WaitingOn != "signal:go" || row.Attributes["tenant"] != "one" || len(records) != 4 || port.bodyReads != 0 {
				t.Fatal("canonical status requires owned history, not input body", row, e, port.bodyReads)
			}
			rows, e := p.ListByAttribute(ctx, "tenant", "one", "suspended")
			if e != nil || len(rows) != 1 {
				t.Fatal(rows, e)
			}
			if lag, e := p.Lag(ctx); e != nil || lag != 0 {
				t.Fatal("read witnesses inflated lag", lag, e)
			}
			if mode == "ordinary" {
				originalUpdated := row.Updated
				forged := row
				forged.Status = "completed"
				forged.Attributes = map[string]string{"tenant": "forged"}
				if e = p.putRow(ctx, forged); e != nil {
					t.Fatal(e)
				}
				port.pinWrites = 0
				if e = p.refreshGraph(ctx, false); e != nil {
					t.Fatal(e)
				}
				repaired, e := p.Get(ctx, h.Type, h.ID)
				if e != nil || repaired.Status != "suspended" || repaired.Attributes["tenant"] != "one" || port.pinWrites != 0 || repaired.Updated.IsZero() || repaired.Updated.After(originalUpdated) {
					t.Fatal("unchanged canonical refresh pinned or trusted query bytes", repaired, e, port.pinWrites)
				}
			}
			if mode == "retired" {
				terminal, _ := json.Marshal(wf.Outcome{InvSeq: h.InvSeq, Result: []byte(`42`)})
				tail, e = store.Append(ctx, h.Type, h.ID, h.InvSeq, journal.Entry{Index: 4, Kind: journal.Completed, Payload: terminal}, tail, nil, nil)
				if e != nil {
					t.Fatal(e)
				}
				if e = store.Retire(ctx, h.Type, h.ID, h.InvSeq, tail); e != nil {
					t.Fatal(e)
				}
				if lag, e := p.Lag(ctx); e != nil || lag != 1 {
					t.Fatal("retired lag", lag, e)
				}
				if e = p.Rebuild(ctx); e != nil {
					t.Fatal(e)
				}
				if _, e = p.Get(ctx, h.Type, h.ID); !errors.Is(e, ErrNotFound) {
					t.Fatal("retired row retained", e)
				}
				rows, e = p.ListByAttribute(ctx, "tenant", "one", "")
				if e != nil || len(rows) != 0 {
					t.Fatal("retired index retained", e)
				}
			}
			t.Log(fmt.Sprintf("mode=%s canonical status/attributes, no input body reads, bounded catalog and lag", mode))
		})
	}
}

func TestGraphVisibilityConfiguration(t *testing.T) {
	for _, namespace := range []string{"", "other"} {
		p := &Projection{graphBucket: "GRAPH_VIEW", postgres: &PostgresStore{Namespace: namespace}}
		if _, err := newGraphProjection(context.Background(), p, nil); err == nil {
			t.Fatal("accepted missing/mismatched PostgreSQL namespace")
		}
	}

	model := sim.NewGraphPublicationTransport(sim.NewScheduler(1))
	store, err := journal.NewGraphStore(journal.GraphConfig{Protocol: model.Protocol(), CanonicalStarts: true, CanonicalSignals: true})
	if err != nil {
		t.Fatal(err)
	}
	for _, bucket := range []string{"", "WF_VIEW", "WF_STATE", "WF_LEASE", "WF_ASSIGN"} {
		if e := WithGraphJournal(store, bucket)(&Projection{}); e == nil {
			t.Fatal("shared graph visibility bucket accepted", bucket)
		}
	}
	if e := WithGraphJournal(nil, "GRAPH_VIEW")(&Projection{}); e == nil {
		t.Fatal("nil authority selected")
	}
	if e := WithGraphRefreshInterval(0)(&Projection{}); e == nil {
		t.Fatal("invalid refresh interval")
	}
}
