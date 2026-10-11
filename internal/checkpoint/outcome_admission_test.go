package checkpoint_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"reflect"
	"testing"

	"js-wf/internal/checkpoint"
	"js-wf/wf"
)

func promiseOutcomeCases() map[string]string {
	cases := map[string]string{
		"inline":                       `{"inv_seq":22,"result":"NDI="}`,
		"user_null":                    `{"inv_seq":22,"result":"bnVsbA=="}`,
		"opaque_user_duplicates":       `{"inv_seq":22,"result":"eyJ1c2VyIjoxLCJ1c2VyIjoyfQ=="}`,
		"explicit_empty_error":         `{"inv_seq":22,"result":"NDI=","error":""}`,
		"legacy_identity_omitted":      `{"result":"NDI="}`,
		"valid_failed":                 `{"inv_seq":22,"error":"failed"}`,
		"opaque_limit_request":         `{"inv_seq":22,"error":"failed","limit_request":{"user":null,"user":2}}`,
		"opaque_limit_payload":         `{"inv_seq":22,"error":"failed","limit_entry":{"kind":"Suspended","payload":{"user":null,"user":2}}}`,
		"failed_inline":                `{"inv_seq":22,"error":"failed","result":"NDI="}`,
		"failed_external":              `{"inv_seq":22,"error":"failed","result_ref":"result-object","result_hash":"` + string(bytes.Repeat([]byte("a"), 64)) + `"}`,
		"external":                     `{"inv_seq":22,"result_ref":"result-object","result_hash":"` + string(bytes.Repeat([]byte("a"), 64)) + `"}`,
		"duplicate_invocation":         `{"inv_seq":9,"inv_seq":22,"result":"NDI="}`,
		"alias_invocation":             `{"Inv_seq":9,"inv_seq":22,"result":"NDI="}`,
		"escaped_duplicate_invocation": `{"\u0069nv_seq":9,"inv_seq":22,"result":"NDI="}`,
		"duplicate_result":             `{"inv_seq":22,"result":"Nw==","result":"NDI="}`,
		"alias_result":                 `{"inv_seq":22,"Result":"Nw==","result":"NDI="}`,
		"duplicate_error":              `{"inv_seq":22,"error":"bad","error":"","result":"NDI="}`,
		"alias_error":                  `{"inv_seq":22,"Error":"bad","error":"","result":"NDI="}`,
		"unknown_field":                `{"inv_seq":22,"foreign":true,"result":"NDI="}`,
		"null_limit_kind":              `{"inv_seq":22,"result":"NDI=","limit_entry":{"kind":null,"payload":{}}}`,
		"null_limit_payload":           `{"inv_seq":22,"result":"NDI=","limit_entry":{"kind":"Suspended","payload":null}}`,
		"duplicate_limit_kind":         `{"inv_seq":22,"result":"NDI=","limit_entry":{"kind":"Failed","kind":"Suspended","payload":{}}}`,
	}
	for _, field := range []string{"inv_seq", "result_ref", "result_hash", "error", "limit_request", "limit_entry", "result"} {
		cases["null_"+field] = `{"inv_seq":22,"` + field + `":null}`
		if field == "inv_seq" {
			cases["null_"+field] = `{"inv_seq":null,"result":"NDI="}`
		}
	}
	return cases
}
func validPromiseOutcomeCase(name string) bool {
	switch name {
	case "inline", "user_null", "opaque_user_duplicates", "explicit_empty_error", "legacy_identity_omitted", "valid_failed", "opaque_limit_request", "opaque_limit_payload", "external":
		return true
	}
	return false
}
func TestCheckpointPromiseOutcomeMetadataAdmission(t *testing.T) {
	for name, outcome := range promiseOutcomeCases() {
		t.Run(name, func(t *testing.T) {
			frame := checkpoint.Frame{Version: checkpoint.Version, Identity: checkpoint.Identity{Type: "parent", ID: "materialized", InvSeq: 17}, Stage: "collect_v1", Data: json.RawMessage(`{"user":null,"user":2}`), Anchor: checkpoint.Anchor{Index: 2, Epoch: 51}, StepPosition: 2, State: map[string]json.RawMessage{"counter": json.RawMessage(`23`)}, PromiseOutcomes: map[string]json.RawMessage{"child_4": json.RawMessage(outcome)}}
			raw, err := json.Marshal(frame)
			if err != nil {
				t.Fatal(err)
			}
			sum := sha256.Sum256(raw)
			hash := hex.EncodeToString(sum[:])
			got, err := checkpoint.Decode(raw, hash, frame.Identity, frame.Anchor)
			if validPromiseOutcomeCase(name) {
				if err != nil || !reflect.DeepEqual(got, frame) {
					t.Fatal("valid frame changed", got, err)
				}
			} else if !errors.Is(err, checkpoint.ErrInvalid) || !reflect.DeepEqual(got, checkpoint.Frame{}) {
				t.Error("malformed promise frame admitted", got, err)
			}
			encoded, encodedHash, encodeErr := checkpoint.Encode(frame)
			if validPromiseOutcomeCase(name) {
				if encodeErr != nil || !bytes.Equal(encoded, raw) || encodedHash != hash {
					t.Fatal("valid frame encode changed", encodeErr)
				}
			} else if !errors.Is(encodeErr, checkpoint.ErrInvalid) || encoded != nil || encodedHash != "" {
				t.Error("malformed promise frame encoded", encodeErr)
			}
			appends := 0
			c, info, err := wf.NewCheckpointContext(context.Background(), nil, func(context.Context, wf.Kind, json.RawMessage) error { appends++; return nil }, raw, wf.CheckpointLocation{Type: frame.Identity.Type, ID: frame.Identity.ID, InvSeq: frame.Identity.InvSeq, Index: frame.Anchor.Index, Epoch: frame.Anchor.Epoch, Hash: hash})
			if appends != 0 {
				t.Fatal("restore appended records", appends)
			}
			if validPromiseOutcomeCase(name) {
				var counter int
				if err != nil || c == nil || info.Stage != frame.Stage || !bytes.Equal(info.Data, frame.Data) {
					t.Fatal("valid promise restore failed", info, err)
				}
				found, err := c.GetState("counter", &counter)
				if err != nil || !found || counter != 23 {
					t.Fatal("restored state differs", counter, err)
				}
			} else if !errors.Is(err, wf.ErrInvalidCheckpoint) || c != nil || !reflect.DeepEqual(info, wf.CheckpointInfo{}) {
				t.Error("malformed promise restored SDK state", info, err)
			}
		})
	}
}
