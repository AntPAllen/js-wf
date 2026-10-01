//go:build linux

package integration_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"sync"
	"testing"
	"time"

	"js-wf/identity"
	"js-wf/journal"
	"js-wf/lease"
	"js-wf/provision"
	"js-wf/testcluster"
	"js-wf/wf"

	"github.com/nats-io/nats.go/jetstream"
)

type leaseAppendPressureRow struct {
	Delayed bool                       `json:"delayed"`
	Append  bool                       `json:"append"`
	Writers int                        `json:"writers"`
	Start   time.Time                  `json:"start"`
	End     time.Time                  `json:"end"`
	Owners  []leaseAppendPressureOwner `json:"owners"`
}

type leaseAppendPressureOwner struct {
	ID             string        `json:"id"`
	Calls          int           `json:"calls"`
	RenewTime      time.Duration `json:"renew_ns"`
	AppendTime     time.Duration `json:"append_ns"`
	MaxRenew       time.Duration `json:"max_renew_ns"`
	Epoch          uint64        `json:"epoch"`
	RevisionBefore uint64        `json:"revision_before"`
	RevisionAfter  uint64        `json:"revision_after"`
	GateTime       time.Duration `json:"gate_ns"`
	UpdateTime     time.Duration `json:"update_ns"`
	Error          string        `json:"error,omitempty"`
}

// Isolate the steady two-replica quorum seen after a mixed heal: lease leader
// healthy node 0, journal leader delayed node 1, node 2 stopped. This measures
// production renewal and CAS append operations, not the worker fault matrix.
// Keep the snapshot-manifest bucket on a survivor: a killed metadata leader
// would add an unrelated election to this steady pressure measurement.
// Eight independent owners test shared lease-group pressure observed in mixed
// recovery; each owner has its own key, epoch, revision gate and journal.
func TestLeaseAppendPressureFixedPlacement(t *testing.T) {
	if os.Getenv("WF_LEASE_APPEND_PRESSURE") != "1" {
		t.Skip("set WF_LEASE_APPEND_PRESSURE=1 for the fixed-placement pressure contract")
	}
	if _, err := exec.LookPath("strace"); err != nil {
		t.Fatal(err)
	}
	cluster, err := testcluster.StartProcesses(t.TempDir(), 3)
	if err != nil {
		t.Fatal(err)
	}
	defer cluster.Close()
	ctx, stop := context.WithTimeout(context.Background(), 4*time.Minute)
	defer stop()
	js, err := jetstream.New(cluster.Clients[0])
	if err != nil {
		t.Fatal(err)
	}
	for ctx.Err() == nil {
		attempt, cancel := context.WithTimeout(ctx, 3*time.Second)
		err = provision.Ensure(attempt, js, 3)
		cancel()
		if err == nil {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if err != nil {
		t.Fatal(err)
	}
	for name, node := range map[string]int{"KV_WF_LEASE": 0, "WF_JRN": 1, "KV_WF_STATE": 0} {
		name, node := name, node
		preferMixedAckLeader(t, ctx, cluster.Clients[0], "$JS.API.STREAM.LEADER.STEPDOWN."+name, fmt.Sprintf("wf-process-%d", node), func(attempt context.Context) (*jetstream.ClusterInfo, error) {
			stream, err := js.Stream(attempt, name)
			if err != nil {
				return nil, err
			}
			info, err := stream.Info(attempt)
			if err != nil {
				return nil, err
			}
			return info.Cluster, nil
		})
	}
	leasing, err := lease.New(ctx, js)
	if err != nil {
		t.Fatal(err)
	}
	store := journal.New(js)
	kv, err := js.KeyValue(ctx, "WF_LEASE")
	if err != nil {
		t.Fatal(err)
	}
	// Warm both data/Raft groups before SlowDisk attaches to existing files.
	warm, err := leasing.Acquire(ctx, "pressure", "warm", "warm")
	if err != nil {
		t.Fatal(err)
	}
	if err := warm.Renew(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Append(ctx, "pressure", "warm", journal.Entry{Epoch: warm.Epoch(), Kind: journal.Started, WorkerID: "warm"}, 0); err != nil {
		t.Fatal(err)
	}
	if err := warm.Release(ctx); err != nil {
		t.Fatal(err)
	}
	if err := waitMatrixWorkflowReplicas(ctx, js); err != nil {
		t.Fatal(err)
	}
	if err := cluster.KillNode(2); err != nil {
		t.Fatal(err)
	}
	var rows []leaseAppendPressureRow
	tracing := false
	defer func() {
		_ = cluster.StopSlowDisk(1)
		if prefix := os.Getenv("LEASE_APPEND_REPORT"); prefix != "" {
			data, err := json.MarshalIndent(rows, "", "  ")
			if err != nil {
				t.Error(err)
			} else if err := os.WriteFile(prefix+".json", data, 0600); err != nil {
				t.Error(err)
			}
			if tracing {
				if data, err := os.ReadFile(cluster.DiskTracePath(1)); err == nil {
					if err := os.WriteFile(prefix+"-disk.log", data, 0600); err != nil {
						t.Error(err)
					}
				} else {
					t.Error(err)
				}
			}
			for _, err := range saveMixedJetStreamDiagnostics(cluster, prefix, "final") {
				t.Error(err)
			}
		}
	}()
	const calls = 48
	digest := sha256.Sum256([]byte(`0`))
	requestPayload, _ := json.Marshal(map[string]string{"kind": "run", "name": "pressure", "input_hash": hex.EncodeToString(digest[:])})
	for _, delayed := range []bool{false, true} {
		if delayed {
			if err := cluster.SlowDisk(1, 70*time.Millisecond); err != nil {
				t.Fatal(err)
			}
			tracing = true
		}
		for _, workload := range []struct {
			append  bool
			writers int
		}{{false, 1}, {true, 1}, {true, 2}, {true, 8}} {
			row := leaseAppendPressureRow{Delayed: delayed, Append: workload.append, Writers: workload.writers, Owners: make([]leaseAppendPressureOwner, workload.writers)}
			owners := make([]*lease.Lease, workload.writers)
			for i := range owners {
				id := fmt.Sprintf("d%t-a%t-w%d-%d", delayed, workload.append, workload.writers, i)
				owners[i], err = leasing.Acquire(ctx, "pressure", id, id)
				if err != nil {
					t.Fatal(err)
				}
				before, err := kv.Get(ctx, identity.Key("pressure", id))
				if err != nil {
					t.Fatal(err)
				}
				row.Owners[i] = leaseAppendPressureOwner{ID: id, Epoch: owners[i].Epoch(), RevisionBefore: before.Revision()}
			}
			row.Start = time.Now().UTC()
			start := make(chan struct{})
			var wg sync.WaitGroup
			for i, owner := range owners {
				wg.Add(1)
				go func(i int, owner *lease.Lease) {
					defer wg.Done()
					<-start
					result := &row.Owners[i]
					var tail uint64
					for n := range calls {
						at := time.Now()
						attempt, cancel := context.WithTimeout(ctx, 3*time.Second)
						renewed, timing, err := owner.RenewTimed(attempt, 0)
						cancel()
						result.GateTime += timing.GateWait
						result.UpdateTime += timing.Update
						if err == nil && (!renewed || !timing.UpdateAttempted) {
							result.Error = "renew skipped its required KV update"
							return
						}
						duration := time.Since(at)
						result.RenewTime += duration
						if duration > result.MaxRenew {
							result.MaxRenew = duration
						}
						if err != nil {
							result.Error = "renew: " + err.Error()
							return
						}
						result.Calls++
						if !workload.append {
							continue
						}
						entry := journal.Entry{Epoch: owner.Epoch(), Index: uint64(n), WorkerID: result.ID}
						if n == 0 {
							entry.Kind = journal.Started
						} else if n == calls-1 {
							entry.Kind = journal.Completed
							entry.Payload, _ = json.Marshal(wf.Outcome{InvSeq: 1, Result: []byte(`true`)})
						} else if n%2 == 1 {
							entry.Kind = journal.StepRequested
							entry.Payload = requestPayload
						} else {
							entry.Kind = journal.StepCompleted
							entry.Payload = json.RawMessage(`{"result":true}`)
						}
						at = time.Now()
						attempt, cancel = context.WithTimeout(ctx, 3*time.Second)
						sequence, err := store.Append(attempt, "pressure", result.ID, entry, tail)
						cancel()
						result.AppendTime += time.Since(at)
						if err != nil {
							result.Error = "append: " + err.Error()
							return
						}
						tail = sequence
					}
				}(i, owner)
			}
			close(start)
			wg.Wait()
			row.End = time.Now().UTC()
			rows = append(rows, row)
			for i, result := range row.Owners {
				if result.Error != "" || result.Calls != calls {
					t.Fatalf("row=%+v", row)
				}
				after, err := kv.Get(ctx, identity.Key("pressure", result.ID))
				var value lease.Value
				if err != nil || json.Unmarshal(after.Value(), &value) != nil || value.Worker != result.ID || value.Epoch != result.Epoch || after.Revision() <= result.RevisionBefore {
					t.Fatalf("confirmed renewal did not preserve/advance owner: owner=%+v value=%+v err=%v", result, value, err)
				}
				rows[len(rows)-1].Owners[i].RevisionAfter = after.Revision()
				if delayed && !workload.append && result.RenewTime < calls*70*time.Millisecond {
					t.Fatalf("renewal missed injected delay lower bound: %s", result.RenewTime)
				}
				if workload.append {
					records, _, err := store.Read(ctx, "pressure", result.ID)
					if err != nil || len(records) != calls || records[calls-1].Kind != journal.Completed {
						t.Fatalf("append audit id=%s records=%d err=%v", result.ID, len(records), err)
					}
					for n, record := range records {
						if record.Index != uint64(n) || record.Epoch != result.Epoch || record.WorkerID != result.ID {
							t.Fatalf("append identity changed: %+v", record)
						}
					}
				}
				if err := owners[i].Release(ctx); err != nil {
					t.Fatal(err)
				}
			}
			for name, node := range map[string]int{"KV_WF_LEASE": 0, "WF_JRN": 1, "KV_WF_STATE": 0} {
				attempt, cancel := context.WithTimeout(ctx, 3*time.Second)
				stream, err := js.Stream(attempt, name)
				var info *jetstream.StreamInfo
				if err == nil {
					info, err = stream.Info(attempt)
				}
				cancel()
				if err != nil || info == nil || info.Cluster == nil || info.Cluster.Leader != fmt.Sprintf("wf-process-%d", node) {
					t.Fatalf("placement changed stream=%s info=%+v err=%v", name, info, err)
				}
			}
			t.Logf("pressure delayed=%v append=%v writers=%d elapsed=%s owners=%+v", delayed, workload.append, workload.writers, row.End.Sub(row.Start), row.Owners)
		}
	}
	if err := cluster.StopSlowDisk(1); err != nil {
		t.Fatal(err)
	}
	trace, err := os.ReadFile(cluster.DiskTracePath(1))
	if err != nil || !bytes.Contains(trace, []byte("DELAYED")) {
		t.Fatalf("missing disk delay evidence: %v", err)
	}
}
