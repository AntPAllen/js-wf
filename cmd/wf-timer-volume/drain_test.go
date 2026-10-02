package main

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestDrainRejectsRetainedSourcesAndIncompleteMetadata(t *testing.T) {
	for _, tc := range []struct {
		name          string
		messages      uint64
		streamErr     bool
		failPartition int
		pending       int
		complete      bool
	}{
		{"retained_sources", 768, false, -1, 0, false},
		{"missing_stream", 0, true, -1, 0, false},
		{"partial_consumers", 0, false, 37, 0, false},
		{"pending_delivery", 0, false, -1, 1, false},
		{"complete_drain", 0, false, -1, 0, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			calls := 0
			audit := inspectDrain(context.Background(), drainPort{
				stream: func(context.Context) (uint64, error) {
					if tc.streamErr {
						return 0, context.DeadlineExceeded
					}
					return tc.messages, nil
				},
				consumer: func(_ context.Context, p uint32) (int, error) {
					calls++
					if int(p) == tc.failPartition {
						return 0, context.DeadlineExceeded
					}
					if p == 63 {
						return tc.pending, nil
					}
					return 0, nil
				},
			}, 64)
			if audit.complete(64) != tc.complete {
				t.Fatalf("incorrect drain verdict: %+v", audit)
			}
			if tc.messages != 0 || tc.streamErr {
				if calls != 0 || audit.Pending != nil {
					t.Fatalf("unobserved consumers reported: %+v", audit)
				}
			}
			if tc.streamErr && audit.Messages != nil {
				t.Fatal("unobserved stream reported zero")
			}
			if tc.failPartition >= 0 && (audit.Pending != nil || audit.ConsumersChecked != 37 || !strings.Contains(audit.Error, "consumer 37 metadata")) {
				t.Fatalf("lost partial audit: %+v", audit)
			}
			path := filepath.Join(t.TempDir(), "drain.jsonl")
			if err := appendDrainAudit(path, audit); err != nil {
				t.Fatal(err)
			}
			data, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			var decoded drainAudit
			if err := json.Unmarshal(data, &decoded); err != nil {
				t.Fatal(err)
			}
			if decoded.complete(64) != tc.complete || decoded.Error != audit.Error {
				t.Fatalf("audit changed on disk: %+v", decoded)
			}
		})
	}
	if (drainAudit{}).complete(64) {
		t.Fatal("default counts certify drain")
	}
}

func TestDrainUsesCallerDeadlineForEveryMetadataRequest(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	calls := 0
	audit := inspectDrain(ctx, drainPort{
		stream: func(c context.Context) (uint64, error) {
			if c != ctx {
				t.Fatal("changed stream context")
			}
			return 0, nil
		},
		consumer: func(c context.Context, p uint32) (int, error) {
			if c != ctx {
				t.Fatal("changed consumer context")
			}
			calls++
			if p == 7 {
				cancel()
			}
			return 0, c.Err()
		},
	}, 64)
	cancel()
	if !errors.Is(ctx.Err(), context.Canceled) || calls != 8 || audit.ConsumersChecked != 7 || audit.complete(64) {
		t.Fatalf("deadline evidence lost: %+v calls=%d", audit, calls)
	}
}
