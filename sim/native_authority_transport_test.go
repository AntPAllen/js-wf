package sim

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"js-wf/internal/graphpublication"
)

var nativeAuthorityModes = []string{"healthy", "witness", "replacement", "exhaustion", "drop", "lost"}

func runNativeAuthority(seed int64, replay *Trace) (trace Trace, err error) {
	s := NewScheduler(seed)
	if replay != nil {
		s, err = ReplayScheduler(*replay)
		if err != nil {
			return trace, err
		}
	}
	defer func() { trace = s.Trace() }()
	if err = s.SetWorkload("native_authority_witness"); err != nil {
		return trace, err
	}
	cases := []string{}
	for _, kind := range []string{"root", "blob"} {
		for _, state := range []string{"absent", "present"} {
			for _, mode := range nativeAuthorityModes {
				cases = append(cases, kind+"/"+state+"/"+mode)
			}
		}
	}
	chosen, err := s.Choose(cases)
	if err != nil {
		return trace, err
	}
	parts := strings.Split(chosen, "/")
	kind, state, mode := parts[0], parts[1], parts[2]
	ctx := context.Background()
	model := NewNativeAuthorityTransport(s)
	authority, err := model.Open(ctx)
	if err != nil {
		return trace, err
	}
	peer, err := model.Open(ctx)
	if err != nil {
		return trace, err
	}
	hash := fmt.Sprintf("%x", sha256.Sum256([]byte("native-payload")))
	owner := "owner"
	identity := "native-root"
	if kind == "blob" {
		identity = fmt.Sprintf("%x", sha256.Sum256([]byte("graph-authority/"+hash+"/"+owner)))
	}
	root := graphpublication.EmptyRoot()
	root.Schema = graphpublication.ApplicationSchema
	root.Application = []byte("base")
	fence := graphpublication.Fence{Hash: hash, Owner: owner, Generation: 1, Phase: "uploading", Intents: map[string]graphpublication.Intent{owner: {Destination: "native-root", Expected: 0, Expires: time.UnixMilli(3600000).UTC(), Locations: []graphpublication.Location{{Kind: "payload", First: 0}}}}}
	var revision uint64
	if state == "present" {
		if kind == "root" {
			_, err = authority.CASRoot(ctx, identity, 0, root)
		} else {
			_, err = authority.CASBlob(ctx, identity, 0, fence)
		}
		if err != nil {
			return trace, err
		}
		revision = 1
	}
	root.Application = []byte("ours")
	if state == "present" {
		fence.Phase = "closed"
		fence.Intents = nil
	}
	before := model.MutationAttempts()
	witnesses := 0
	wantAttempts := 1
	if mode == "witness" || mode == "exhaustion" {
		count, e := s.Choose([]string{"1", "3", "7"})
		if e != nil {
			return trace, e
		}
		limit := map[string]int{"1": 1, "3": 3, "7": 7}[count]
		if mode == "exhaustion" {
			limit = 17
			wantAttempts = 16
		} else {
			wantAttempts = limit + 1
		}
		var intervene func(context.Context) error
		intervene = func(c context.Context) error {
			if kind == "root" {
				_, err = peer.ReadRoot(c, identity)
			} else {
				_, err = peer.ReadBlob(c, identity)
			}
			if err != nil {
				return err
			}
			witnesses++
			if witnesses < limit {
				model.BeforeMutation(intervene)
			}
			return nil
		}
		model.BeforeMutation(intervene)
	}
	var replacementRoot graphpublication.Root
	var replacementBlob graphpublication.Record
	if mode == "replacement" {
		wantAttempts = 2 // One peer write and one rejected prepared write.
		model.BeforeMutation(func(c context.Context) error {
			if kind == "root" {
				next := root
				next.Application = []byte("peer")
				replacementRoot, err = peer.CASRoot(c, identity, revision, next)
			} else {
				next := fence
				if state == "absent" {
					intent := next.Intents[owner]
					intent.Destination = "peer"
					next.Intents = map[string]graphpublication.Intent{owner: intent}
				}
				replacementBlob, err = peer.CASBlob(c, identity, revision, next)
			}
			return err
		})
	}
	if mode == "drop" || mode == "lost" {
		fault := DropBeforeCommit
		if mode == "lost" {
			fault = LoseAckAfterCommit
		}
		if err = model.FaultNextMutation(fault); err != nil {
			return trace, err
		}
	}
	var resultRoot graphpublication.Root
	var resultBlob graphpublication.Record
	var mutationErr error
	if kind == "root" {
		resultRoot, mutationErr = authority.CASRoot(ctx, identity, revision, root)
	} else {
		resultBlob, mutationErr = authority.CASBlob(ctx, identity, revision, fence)
	}
	attempts := model.MutationAttempts() - before
	if attempts != wantAttempts {
		return trace, fmt.Errorf("mode=%s attempts=%d want=%d", chosen, attempts, wantAttempts)
	}
	switch mode {
	case "healthy", "witness":
		if mutationErr != nil {
			return trace, mutationErr
		}
		if kind == "root" {
			if resultRoot.Head != revision+1 || string(resultRoot.Application) != "ours" {
				return trace, fmt.Errorf("root mutation differs: %+v", resultRoot)
			}
		} else if resultBlob.Revision != revision+1 || !reflect.DeepEqual(resultBlob.Fence, fence) {
			return trace, fmt.Errorf("blob mutation differs: %+v", resultBlob)
		}
	case "replacement", "exhaustion":
		if mutationErr != graphpublication.ErrConflict {
			return trace, fmt.Errorf("expected definite conflict: %v", mutationErr)
		}
	default:
		if !errors.Is(mutationErr, nats.ErrTimeout) || errors.Is(mutationErr, graphpublication.ErrConflict) {
			return trace, fmt.Errorf("unknown classified/retried: %v", mutationErr)
		}
	}
	if mutationErr != nil && (!reflect.DeepEqual(resultRoot, graphpublication.Root{}) || !reflect.DeepEqual(resultBlob, graphpublication.Record{})) {
		return trace, fmt.Errorf("failed mutation returned authority")
	}
	wantRevision := revision
	if mode == "healthy" || mode == "witness" || mode == "replacement" || mode == "lost" {
		wantRevision++
	}
	if kind == "root" {
		current, e := peer.ReadRoot(ctx, identity)
		if e != nil || current.Head != wantRevision {
			return trace, fmt.Errorf("observed root revision=%d want=%d: %v", current.Head, wantRevision, e)
		}
		if mode == "replacement" && !reflect.DeepEqual(current, replacementRoot) {
			return trace, fmt.Errorf("peer root overwritten")
		}
		if (mode == "healthy" || mode == "witness" || mode == "lost") && string(current.Application) != "ours" {
			return trace, fmt.Errorf("published root image differs")
		}
	} else {
		current, e := peer.ReadBlob(ctx, identity)
		if e != nil || current.Revision != wantRevision {
			return trace, fmt.Errorf("observed blob revision=%d want=%d: %v", current.Revision, wantRevision, e)
		}
		if mode == "replacement" && !reflect.DeepEqual(current, replacementBlob) {
			return trace, fmt.Errorf("peer blob overwritten")
		}
		if (mode == "healthy" || mode == "witness" || mode == "lost") && !reflect.DeepEqual(current.Fence, fence) {
			return trace, fmt.Errorf("published blob image differs")
		}
	}
	s.RecordTransport(TransportEvent{Operation: "check_native_authority_witness", Expected: revision, Sequence: wantRevision, Outcome: chosen + fmt.Sprintf("/attempts=%d/witnesses=%d", attempts, witnesses)})
	if err = s.Finish(); err != nil {
		return trace, err
	}
	return trace, nil
}

func TestSeededGraphNativeAuthorityReplay(t *testing.T) {
	observed := map[string]int{}
	fail := func(seed int64, trace Trace, err error) {
		path, e := saveSeedFailureTrace(t.Name(), seed, trace)
		if e != nil {
			t.Fatal(e)
		}
		t.Fatalf("FAULT_SEED=%d FAULT_TRACE=%s: %v", seed, path, err)
	}
	for seed := range seededSchedules(t) {
		generated, err := runNativeAuthority(seed, nil)
		if err != nil {
			fail(seed, generated, err)
		}
		replayed, err := runNativeAuthority(seed, &generated)
		if err != nil || !reflect.DeepEqual(generated, replayed) {
			fail(seed, generated, fmt.Errorf("native authority replay differs: %v", err))
		}
		mode := generated.Decisions[0].Chosen
		observed[mode]++
		if dir := os.Getenv("SIM_NATIVE_AUTHORITY_ROOT"); dir != "" && observed[mode] == 1 {
			if err := generated.Save(filepath.Join(dir, "native-authority-"+strings.ReplaceAll(mode, "/", "-")+".json")); err != nil {
				t.Fatal(err)
			}
		}
	}
	if len(observed) != 24 {
		t.Fatalf("native authority modes=%v", observed)
	}
	t.Logf("production native adapter: %v; physical witness fencing, unchanged logical revisions, peer replacement rejection, bounded retries and no uncertain republication", observed)
}
