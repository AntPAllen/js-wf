package integration_test

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"js-wf/retention"
)

// A mark/list failure must be detected before any eligible orphan is deleted.
func TestQuiescentBlobSweepRejectsInvalidRetainedMetadata(t *testing.T) {
	for _, mode := range []string{"json", "name", "bucket", "state_operation", "store_subject"} {
		t.Run(mode, func(t *testing.T) {
			all, _ := setup(t)
			ctx, stop := context.WithTimeout(context.Background(), 20*time.Second)
			defer stop()
			objects, err := all[0].ObjectStore(ctx, "WF_BLOB")
			if err != nil {
				t.Fatal(err)
			}
			for _, name := range []string{"input-corrupt", "input-safe"} {
				if _, err := objects.PutBytes(ctx, name, []byte(name)); err != nil {
					t.Fatal(err)
				}
			}
			marker := "object metadata"
			if mode == "store_subject" {
				marker = "unknown object store subject"
				stream, err := all[0].Stream(ctx, "OBJ_WF_BLOB")
				if err != nil {
					t.Fatal(err)
				}
				info, err := stream.Info(ctx)
				if err != nil {
					t.Fatal(err)
				}
				info.Config.Subjects = append(info.Config.Subjects, "$O.WF_BLOB.X.>")
				if _, err := all[0].UpdateStream(ctx, info.Config); err != nil {
					t.Fatal(err)
				}
				if _, err := all[0].Publish(ctx, "$O.WF_BLOB.X.unrecognized", []byte(`{}`)); err != nil {
					t.Fatal(err)
				}
			} else if mode == "state_operation" {
				marker = "unknown state operation"
				message := &nats.Msg{Subject: "$KV.WF_STATE.test.reference", Header: nats.Header{}, Data: []byte(`{"result_ref":"input-corrupt"}`)}
				message.Header.Set("KV-Operation", "UNKNOWN")
				if _, err := all[0].PublishMsg(ctx, message); err != nil {
					t.Fatal(err)
				}
			} else {
				stream, err := all[0].Stream(ctx, "OBJ_WF_BLOB")
				if err != nil {
					t.Fatal(err)
				}
				subject := "$O.WF_BLOB.M." + base64.URLEncoding.EncodeToString([]byte("input-corrupt"))
				original, err := stream.GetLastMsgForSubject(ctx, subject)
				if err != nil {
					t.Fatal(err)
				}
				var value map[string]any
				if err := json.Unmarshal(original.Data, &value); err != nil {
					t.Fatal(err)
				}
				switch mode {
				case "name":
					value["name"] = "input-other"
				case "bucket":
					value["bucket"] = "OTHER"
				}
				data, err := json.Marshal(value)
				if err != nil {
					t.Fatal(err)
				}
				if mode == "json" {
					data = []byte("invalid JSON")
				}
				message := &nats.Msg{Subject: subject, Header: nats.Header{}, Data: data}
				message.Header.Set("Nats-Rollup", "sub")
				if _, err := all[0].PublishMsg(ctx, message); err != nil {
					t.Fatal(err)
				}
			}
			result, err := retention.SweepBlobsQuiescent(ctx, all[0], 0, time.Now().Add(time.Minute))
			if err == nil || !strings.Contains(err.Error(), marker) || result.Deleted != 0 {
				t.Fatalf("metadata guard: result=%+v err=%v", result, err)
			}
			data, err := objects.GetBytes(ctx, "input-safe")
			if err != nil || string(data) != "input-safe" {
				t.Fatalf("orphan deleted before validation: data=%q err=%v", data, err)
			}
		})
	}
}
