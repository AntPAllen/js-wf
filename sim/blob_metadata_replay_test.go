package sim

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"js-wf/retention"
)

// Raw metadata decoding uses the production adapter's validation functions.
// The underlying model supplies retained KV/object state, not watcher delivery.
type metadataSweepPort struct {
	*BlobSweepTransport
	schedule *Scheduler
	fault    string
}

func (p *metadataSweepPort) StateValue(ctx context.Context, key string) ([]byte, error) {
	data, err := p.BlobSweepTransport.StateValue(ctx, key)
	if err != nil {
		return nil, err
	}
	message := retention.BlobSweepMessage{Subject: "$KV.WF_STATE." + key, Header: nats.Header{}, Data: data}
	if p.fault == "state_operation" {
		message.Header.Set("KV-Operation", "UNKNOWN")
	}
	data, err = retention.DecodeBlobSweepStateMetadata(key, message)
	outcome := "ok"
	if err != nil {
		outcome = err.Error()
	}
	p.schedule.RecordTransport(TransportEvent{Operation: "blob_state_metadata", Subject: key, Outcome: outcome, AtMillis: p.schedule.NowMillis()})
	return data, err
}
func (p *metadataSweepPort) Objects(ctx context.Context) ([]retention.BlobSweepObject, error) {
	objects, err := p.BlobSweepTransport.Objects(ctx)
	if err != nil {
		return nil, err
	}
	for i, object := range objects {
		subject := "$O.WF_BLOB.M." + base64.URLEncoding.EncodeToString([]byte(object.Name))
		info := map[string]any{"name": object.Name, "bucket": "WF_BLOB"}
		if i == 0 {
			switch p.fault {
			case "name":
				info["name"] = "input-other"
			case "bucket":
				info["bucket"] = "OTHER"
			}
		}
		data, err := json.Marshal(info)
		if err != nil {
			return nil, err
		}
		if i == 0 && p.fault == "json" {
			data = []byte("invalid JSON")
		}
		decoded, live, err := retention.DecodeBlobSweepObjectMetadata(subject, retention.BlobSweepMessage{Subject: subject, Data: data}, object.ModTime)
		outcome := "ok"
		if err != nil {
			outcome = err.Error()
		}
		p.schedule.RecordTransport(TransportEvent{Operation: "blob_object_metadata", Subject: subject, DataSHA256: digest(data), Outcome: outcome, AtMillis: p.schedule.NowMillis()})
		if err != nil {
			return nil, err
		}
		if !live {
			return nil, fmt.Errorf("live model object decoded as deleted")
		}
		objects[i] = decoded
	}
	return objects, nil
}
func runSeededBlobMetadata(seed int64, replay *Trace) (trace Trace, runErr error) {
	schedule := NewScheduler(seed)
	if replay != nil {
		var err error
		schedule, err = ReplayScheduler(*replay)
		if err != nil {
			return trace, err
		}
	}
	defer func() { trace = schedule.Trace() }()
	if err := schedule.SetWorkload("blob_metadata"); err != nil {
		return trace, err
	}
	mode, err := schedule.Choose([]string{"json", "name", "bucket", "state_operation"})
	if err != nil {
		return trace, err
	}
	ctx := context.Background()
	now := time.Unix(0, 0).UTC().Add(time.Hour)
	model := NewBlobSweepTransport(schedule)
	for _, name := range []string{"input-kept", "input-orphan"} {
		model.PutObject(name, []byte(name), now.Add(-time.Minute))
	}
	if _, err := model.State().Create(ctx, "test.reference", []byte(`{"result_ref":"input-kept"}`)); err != nil {
		return trace, err
	}
	port := &metadataSweepPort{BlobSweepTransport: model, schedule: schedule, fault: mode}
	result, err := retention.SweepBlobsQuiescentWithPort(ctx, port, 0, now)
	marker := "object metadata"
	if mode == "state_operation" {
		marker = "unknown state operation"
	}
	if err == nil || !strings.Contains(err.Error(), marker) || result.Deleted != 0 || !model.HasObject("input-kept") || !model.HasObject("input-orphan") {
		return trace, fmt.Errorf("metadata guard mode=%s result=%+v err=%v", mode, result, err)
	}
	// Repair the raw metadata and retry exactly the same production sweep.
	port.fault = ""
	recovered, err := retention.SweepBlobsQuiescentWithPort(ctx, port, 0, now)
	if err != nil || recovered != (retention.BlobSweepResult{Objects: 2, Referenced: 1, Eligible: 1, Deleted: 1}) || !model.HasObject("input-kept") || model.HasObject("input-orphan") {
		return trace, fmt.Errorf("metadata repair mode=%s result=%+v err=%v", mode, recovered, err)
	}
	schedule.RecordTransport(TransportEvent{Operation: "check_blob_metadata", Outcome: mode, Sequence: uint64(recovered.Deleted), AtMillis: schedule.NowMillis()})
	if err := schedule.Finish(); err != nil {
		return trace, err
	}
	return trace, nil
}
func TestSeededBlobMetadataReplay(t *testing.T) {
	if os.Getenv("SIM_BLOB_METADATA_HELPER") == "1" {
		seed, err := strconv.ParseInt(os.Getenv("FAULT_SEED"), 10, 64)
		if err != nil {
			t.Fatal(err)
		}
		trace, err := runSeededBlobMetadata(seed, nil)
		if err != nil {
			t.Fatal(err)
		}
		if err := trace.Save(os.Getenv("SIM_BLOB_METADATA_OUT")); err != nil {
			t.Fatal(err)
		}
		return
	}
	observed := map[string]int{}
	for seed, limit := int64(1), seededScheduleLimit(t); seed <= limit; seed++ {
		trace, err := runSeededBlobMetadata(seed, nil)
		if err != nil {
			root := os.Getenv("FAULT_TRACE_OUT")
			if root == "" {
				dir, mkdirErr := os.MkdirTemp("", "js-wf-blob-metadata-failure-")
				if mkdirErr != nil {
					t.Fatal(mkdirErr)
				}
				root = filepath.Join(dir, "trace.json")
			}
			_ = trace.Save(root)
			t.Fatalf("FAULT_SEED=%d FAULT_TRACE=%s: %v", seed, root, err)
		}
		observed[trace.Decisions[0].Chosen]++
		if seed <= 10 {
			again, err := runSeededBlobMetadata(seed, &trace)
			if err != nil || !reflect.DeepEqual(trace, again) {
				t.Fatalf("seed %d replay: %v", seed, err)
			}
		}
	}
	for _, mode := range []string{"json", "name", "bucket", "state_operation"} {
		if observed[mode] == 0 {
			t.Fatalf("mode %s not covered", mode)
		}
	}
	var paths [2]string
	for i := range paths {
		paths[i] = filepath.Join(t.TempDir(), "metadata.json")
		cmd := exec.Command(os.Args[0], "-test.run=^TestSeededBlobMetadataReplay$")
		cmd.Env = append(os.Environ(), "SIM_BLOB_METADATA_HELPER=1", "FAULT_SEED=42", "SIM_BLOB_METADATA_OUT="+paths[i])
		if output, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("child %d: %v %s", i, err, output)
		}
	}
	a, err := os.ReadFile(paths[0])
	if err != nil {
		t.Fatal(err)
	}
	b, err := os.ReadFile(paths[1])
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(a, b) {
		t.Fatal("metadata replay changes across processes")
	}
}
