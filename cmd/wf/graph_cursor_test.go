package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	"js-wf/client"
	"js-wf/internal/checkpoint"
	"js-wf/internal/graphpublication"
	"js-wf/journal"
	"js-wf/provision"
	"js-wf/testcluster"
)

func TestGraphOperatorCursorRejectsUnknownVersion(t *testing.T) {
	for _, version := range []string{"0", "3", "7"} {
		err := run([]string{"-graph-cursor-version", version, "export-journal", "flow", "id"}, &bytes.Buffer{})
		if err == nil || !strings.Contains(err.Error(), "must be 4, 5 or 6") {
			t.Fatal(version, err)
		}
	}
}

// Inspect actual owned history across both indexed schemas. In v6 the original
// journal bodies are physically collected before any operator command runs.
func TestNativeGraphCursorOperatorHistory(t *testing.T) {
	for _, version := range []int{4, 5, 6} {
		for _, replicas := range []int{1, 3} {
			t.Run(fmt.Sprintf("v%d/R%d-domain", version, replicas), func(t *testing.T) {
				const domain = "CURSOROPS"
				cluster, err := testcluster.StartWithDomain(t.TempDir(), replicas, domain)
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(cluster.Close)
				ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
				defer cancel()
				if replicas > 1 {
					for {
						ready := false
						for _, server := range cluster.Servers {
							ready = ready || server.JetStreamIsLeader() && len(server.JetStreamClusterPeers()) == replicas
						}
						if ready {
							break
						}
						select {
						case <-ctx.Done():
							t.Fatal(ctx.Err())
						case <-time.After(20 * time.Millisecond):
						}
					}
				}
				js, err := jetstream.NewWithDomain(cluster.Clients[0], domain)
				if err != nil {
					t.Fatal(err)
				}
				if err = provision.Ensure(ctx, js, replicas); err != nil {
					t.Fatal(err)
				}
				cfg := journal.NativeGraphConfig{AuthorityStream: "CURSOR_AUTH", AuthorityPrefix: "wf.graph.cursor", ObjectBucket: "CURSOR_OBJECTS", ExpectedReplicas: replicas, CanonicalStarts: true, CanonicalSignals: true, CheckpointIndex: version >= 5, ArchiveCheckpoints: version == 6, IntentTTL: time.Second}
				configs, err := journal.NativeGraphStreamConfigs(cfg, replicas)
				if err != nil {
					t.Fatal(err)
				}
				for _, config := range configs {
					if _, err = js.CreateStream(ctx, config); err != nil {
						t.Fatal(err)
					}
				}
				store, err := journal.OpenNativeGraphStore(ctx, js, cfg)
				if err != nil {
					t.Fatal(err)
				}
				sdk, err := client.NewWithGraphJournal(js, store)
				if err != nil {
					t.Fatal(err)
				}
				h, err := sdk.Start(ctx, "flow", "cursor", []byte(`7`))
				if err != nil {
					t.Fatal(err)
				}
				status, err := store.InspectStart(ctx, h.Type, h.ID)
				if err != nil {
					t.Fatal(err)
				}
				tail, err := store.Begin(ctx, h.Type, h.ID, h.InvSeq)
				if err != nil {
					t.Fatal(err)
				}
				index := uint64(0)
				appendRecord := func(kind journal.Kind, payload []byte, objects ...[]byte) {
					t.Helper()
					tail, err = store.Append(ctx, h.Type, h.ID, h.InvSeq, journal.Entry{Kind: kind, Index: index, Epoch: 3, Payload: payload}, tail, objects, nil)
					if err != nil {
						t.Fatal(err)
					}
					index++
				}
				started, _ := json.Marshal(map[string]string{"input_sha256": status.State.Start.InputSHA256})
				appendRecord(journal.Started, started, []byte(`7`))
				for i := 0; i < 2; i++ {
					appendRecord(journal.StepRequested, []byte(fmt.Sprintf(`{"kind":"run","name":"padding%d"}`, i)))
					appendRecord(journal.StepCompleted, []byte(`{"result":7}`))
				}
				locals := json.RawMessage(`{"value":42}`)
				digest := sha256.Sum256(locals)
				frame := checkpoint.Frame{Version: checkpoint.Version, Identity: checkpoint.Identity{Type: h.Type, ID: h.ID, InvSeq: h.InvSeq}, Stage: "next", Data: locals, Anchor: checkpoint.Anchor{Index: index + 1, Epoch: 3}, StepPosition: index + 1}
				body, hash, err := checkpoint.Encode(frame)
				if err != nil {
					t.Fatal(err)
				}
				request, _ := json.Marshal(map[string]string{"kind": "checkpoint", "name": "next", "input_hash": hex.EncodeToString(digest[:])})
				completion, _ := json.Marshal(map[string]string{"result_ref": "step-result-" + hash, "result_hash": hash})
				appendRecord(journal.StepRequested, request)
				appendRecord(journal.StepCompleted, completion, body)
				appendRecord(journal.Suspended, []byte(`{"waiting_on":"continuation:next"}`))
				view, err := store.Open(ctx, h.Type, h.ID, h.InvSeq)
				if err != nil {
					t.Fatal(err)
				}
				found, err := view.ReadCheckpoint(ctx, h.Type, h.ID)
				if err != nil || found == nil {
					t.Fatal(found, err)
				}
				original, err := view.Read(ctx, 0)
				if err != nil {
					t.Fatal(err)
				}
				if err = view.Close(ctx); err != nil {
					t.Fatal(err)
				}
				if version >= 5 {
					if err = store.PublishCheckpoint(ctx, h.Type, h.ID, found.Runtime, tail); err != nil {
						t.Fatal(err)
					}
				}
				want, _, err := store.Read(ctx, h.Type, h.ID, h.InvSeq)
				if err != nil {
					t.Fatal(err)
				}
				authority, err := graphpublication.OpenNativeAuthority(ctx, js, cfg.AuthorityStream, cfg.AuthorityPrefix)
				if err != nil {
					t.Fatal(err)
				}
				port, err := graphpublication.OpenNativePort(ctx, authority, cfg.ObjectBucket)
				if err != nil {
					t.Fatal(err)
				}
				if version == 6 {
					if err = store.CompactCheckpoint(ctx, h.Type, h.ID, found.Runtime, tail); err != nil {
						t.Fatal(err)
					}
					if _, err = (graphpublication.Protocol{Port: port}).SweepWithReaders(ctx, time.Now().Add(2*time.Minute)); err != nil {
						t.Fatal(err)
					}
					if _, err = port.Get(ctx, original.EntryBlob, journal.MaxGraphEntryBytes); err == nil {
						t.Fatal("original entry survived collection")
					}
				}
				keys, err := port.RootKeys(ctx)
				if err != nil || len(keys) != 1 {
					t.Fatal(keys, err)
				}
				root, err := port.ReadRoot(ctx, keys[0])
				if err != nil {
					t.Fatal(err)
				}
				var cursor struct {
					Schema       string `json:"schema"`
					RetainedFrom uint64 `json:"retained_from"`
				}
				if json.Unmarshal(root.Application, &cursor) != nil || !strings.HasSuffix(cursor.Schema, fmt.Sprint("v", version)) || (version == 6 && cursor.RetainedFrom != 5) {
					t.Fatal(cursor)
				}
				base := []string{"-url", cluster.Servers[0].ClientURL(), "-domain", domain, "-replicas", strconv.Itoa(replicas), "-graph-authority-stream", cfg.AuthorityStream, "-graph-authority-prefix", cfg.AuthorityPrefix, "-graph-object-bucket", cfg.ObjectBucket}
				var wrongDomain, legacy atomic.Int64
				option := jetstream.WithClientTrace(&jetstream.ClientTrace{RequestSent: func(subject string, _ []byte) {
					if !strings.HasPrefix(subject, "$JS."+domain+".API.") {
						wrongDomain.Add(1)
					}
					if strings.Contains(subject, "WF_JRN") {
						legacy.Add(1)
					}
				}})
				for _, wrong := range []int{4, 5, 6} {
					if wrong == version {
						continue
					}
					var out bytes.Buffer
					args := append(append([]string(nil), base...), "-graph-cursor-version", strconv.Itoa(wrong), "export-journal", h.Type, h.ID)
					if err = runWithJetStreamOptions(args, &out, option); !errors.Is(err, journal.ErrGap) || out.Len() != 0 {
						t.Fatal("wrong schema exported history", wrong, err, out.String())
					}
					after, err := port.ReadRoot(ctx, keys[0])
					if err != nil || !reflect.DeepEqual(root, after) {
						t.Fatal("wrong schema changed logical authority", err)
					}
				}
				for _, command := range []string{"export-journal", "describe"} {
					var out bytes.Buffer
					args := append([]string(nil), base...)
					if version != 4 {
						args = append(args, "-graph-cursor-version", strconv.Itoa(version))
					}
					args = append(args, command, h.Type, h.ID)
					if err = runWithJetStreamOptions(args, &out, option); err != nil {
						t.Fatal(command, err)
					}
					var records []journal.Record
					if command == "describe" {
						var description struct {
							Invocation uint64           `json:"inv_seq"`
							Journal    []journal.Record `json:"journal"`
						}
						if err = json.Unmarshal(out.Bytes(), &description); err != nil || description.Invocation != h.InvSeq {
							t.Fatal(err, description)
						}
						records = description.Journal
					} else if err = json.Unmarshal(out.Bytes(), &records); err != nil {
						t.Fatal(err)
					}
					normalize := func(records []journal.Record) []journal.Record {
						result := append([]journal.Record(nil), records...)
						for i := range result {
							var compact bytes.Buffer
							if err := json.Compact(&compact, result[i].Payload); err != nil {
								t.Fatal(err)
							}
							result[i].Payload = append(json.RawMessage(nil), compact.Bytes()...)
						}
						return result
					}
					if !reflect.DeepEqual(normalize(records), normalize(want)) {
						t.Fatal("operator changed logical history", command, records, want)
					}
				}
				final, err := port.ReadRoot(ctx, keys[0])
				if err != nil || len(final.Readers) != 0 {
					t.Fatal("operator reader leaked", err)
				}
				if wrongDomain.Load() != 0 || legacy.Load() != 0 {
					t.Fatal("operator API namespace mismatch", wrongDomain.Load(), legacy.Load())
				}
				t.Logf("NATIVE_CURSOR_OPERATOR version=%d records=%d retained_from=%d original_entry_collected=%t incorrect_schemas_rejected=2 domain_errors=0 legacy_journal_requests=0 readers_after=0", version, len(want), cursor.RetainedFrom, version == 6)
			})
		}
	}
}
