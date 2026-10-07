//go:build linux

package integrity

import (
	"context"
	"strings"
	"testing"
	"time"
)

func TestCopiedCreationMetadataDeadlineDoesNotReplaceWatchParent(t *testing.T) {
	parent, stop := context.WithTimeout(context.Background(), 20*time.Second)
	defer stop()
	deadline, _ := parent.Deadline()
	state := &copiedCreationState{nativeCreationState: &nativeCreationState{parent: deadline, proof: map[string]any{}}}
	js := copiedCreationJS{state: state}
	metadata, stopMetadata := context.WithTimeout(parent, 2*time.Second)
	defer stopMetadata()
	got, err := js.KeyValue(metadata, "WF_STATE")
	if err != nil || got != state || !state.parent.Equal(deadline) {
		t.Fatalf("metadata replaced full watch deadline: state=%v err=%v parent=%s", got, err, state.parent)
	}
	if short, ok := state.proof["metadata_deadline"].(time.Time); !ok || !short.Before(deadline) {
		t.Fatal("short metadata admission deadline was not recorded separately")
	}
}

func TestNativeCreationFixtureDeadlineErrorReturnsFromSnapshotGoroutine(t *testing.T) {
	parent, stop := context.WithTimeout(context.Background(), 20*time.Second)
	defer stop()
	deadline, _ := parent.Deadline()
	short, stopShort := context.WithTimeout(parent, 2*time.Second)
	defer stopShort()
	state := &nativeCreationState{parent: deadline}
	done := make(chan error, 1)
	go func() {
		_, err := state.WatchAll(short)
		done <- err
	}()
	select {
	case err := <-done:
		if err == nil || !strings.Contains(err.Error(), "changed the full initial-set deadline") {
			t.Fatalf("fixture error=%v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("fixture error exited the snapshot goroutine without returning its result")
	}
}
