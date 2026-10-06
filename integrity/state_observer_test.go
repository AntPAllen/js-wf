package integrity

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/nats-io/nats.go/jetstream"
)

type observerFailureState struct {
	jetstream.KeyValue
	err error
}

func (s observerFailureState) WatchAll(context.Context, ...jetstream.WatchOpt) (jetstream.KeyWatcher, error) {
	return nil, s.err
}

func TestStateObserverDoesNotCertifyPartialOrFailedCreation(t *testing.T) {
	for _, complete := range []bool{true, false} {
		t.Run(fmt.Sprint(complete), func(t *testing.T) {
			watch := &auditWatch{updates: make(chan jetstream.KeyValueEntry, 2)}
			watch.updates <- auditWatchEntry{key: "x", rev: 17}
			if complete {
				watch.updates <- nil
			}
			close(watch.updates)
			var frames []StateSnapshotObservation
			ctx := WithStateSnapshotObserver(t.Context(), func(frame StateSnapshotObservation) { frames = append(frames, frame) })
			state, err := initialAuditState(ctx, auditWatchState{watch: watch}, func(string) bool { return true })
			if (err == nil) != complete || (state != nil) != complete || !watch.stopped {
				t.Fatalf("changed result: state=%v err=%v stopped=%v", state, err, watch.stopped)
			}
			barriers, stops := 0, 0
			for index, frame := range frames {
				if index > 0 && frame.ElapsedNS < frames[index-1].ElapsedNS {
					t.Fatal("observation clock went backwards")
				}
				if frame.Event == "initial_complete" {
					barriers++
				}
				if frame.Event == "watch_stopped" {
					stops++
					if frame.InitialComplete != complete || frame.Received != 1 || frame.Included != 1 || frame.LastRevision != 17 {
						t.Fatalf("wrong stop evidence: %+v", frame)
					}
				}
			}
			if stops != 1 || (barriers == 1) != complete || frames[len(frames)-1].Event != "attempt_return" {
				t.Fatalf("wrong lifecycle: %+v", frames)
			}
		})
	}
	problem := errors.New("watch creation failed")
	var frames []StateSnapshotObservation
	ctx := WithStateSnapshotObserver(t.Context(), func(frame StateSnapshotObservation) { frames = append(frames, frame) })
	state, err := initialAuditState(ctx, observerFailureState{err: problem}, func(string) bool { return true })
	if state != nil || !errors.Is(err, problem) || len(frames) != 3 || frames[1].Event != "watch_creation_error" || frames[2].InitialComplete {
		t.Fatalf("creation identity changed: state=%v err=%v frames=%+v", state, err, frames)
	}
}

func TestStateObserverSamplesCountsWithoutRetainingExcludedValues(t *testing.T) {
	watch := &auditWatch{updates: make(chan jetstream.KeyValueEntry, 10001)}
	for i := 1; i <= 10000; i++ {
		key := "excluded"
		if i%2 == 0 {
			key = fmt.Sprint(i)
		}
		watch.updates <- auditWatchEntry{key: key, rev: uint64(i)}
	}
	watch.updates <- nil
	var frames []StateSnapshotObservation
	ctx := WithStateSnapshotObserver(t.Context(), func(frame StateSnapshotObservation) { frames = append(frames, frame) })
	value, err := initialAuditState(ctx, auditWatchState{watch: watch}, func(key string) bool { return key != "excluded" })
	if err != nil || len(value.(*auditStateSnapshot).values) != 5000 || len(frames) != 7 {
		t.Fatalf("snapshot or bounded observations changed: %v frames=%d", err, len(frames))
	}
	frame := frames[2]
	if frame.Event != "progress" || frame.Received != 10000 || frame.Included != 5000 || frame.LastRevision != 10000 || frame.InitialComplete {
		t.Fatalf("wrong progress counts: %+v", frame)
	}
	_, err = value.Get(context.Background(), "excluded")
	if !errors.Is(err, jetstream.ErrKeyNotFound) {
		t.Fatal("excluded state retained")
	}
}
