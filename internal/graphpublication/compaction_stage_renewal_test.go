package graphpublication

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"testing"
	"time"
)

func TestGraphCompactionStageRenewalPauseResumeAndFences(t *testing.T) {
	for _, mode := range []string{"normal", "resume-before", "resume-after", "repeated", "empty", "complete", "cancel", "drop", "lost", "upload-drop", "upload-lost", "expired", "collector", "append"} {
		t.Run(mode, func(t *testing.T) {
			m, p, root := stageFixture(t, "stage-renew", 12)
			original := copyCompactionRoot(root)
			oldExpiry, nextExpiry := epoch.Add(time.Minute), epoch.Add(3*time.Minute)
			now := epoch.Add(20 * time.Second)
			clock := func() time.Time { return now }
			stage, err := p.BeginPrefixCompaction(ctx, "owner", root.Head, 5, 1024, oldExpiry, []byte("cursor"))
			if err != nil {
				t.Fatal(err)
			}
			prefix := uint64(4)
			if mode == "empty" {
				prefix = 0
			}
			if mode == "complete" {
				prefix = 12
			}
			if prefix > 0 {
				_, done, err := stage.Advance(ctx, prefix)
				if err != nil || done != (prefix == 12) || stage.NextIndex() != prefix {
					t.Fatal(done, err)
				}
			}
			saved := stageCheckpoint(t, stage)
			if mode == "upload-drop" || mode == "upload-lost" {
				m.putHook = func(name string, data []byte) error {
					if mode == "upload-lost" {
						m.objects[name] = bytes.Clone(data)
					}
					return lostReply
				}
				if _, done, err := stage.Advance(ctx, 2); done || err == nil || stage.NextIndex() != prefix {
					t.Fatal("failed record escaped", done, err)
				}
				if _, err := stage.BeginIntentRenewal(ctx, clock, nextExpiry); err == nil {
					t.Fatal("failed stage renewed in place")
				}
				saved = stageCheckpoint(t, stage)
				m.putHook = nil
				stage, err = p.ResumePrefixCompaction(ctx, saved)
				if err != nil {
					t.Fatal(err)
				}
			}
			if mode == "resume-before" {
				stage, err = p.ResumePrefixCompaction(ctx, saved)
				if err != nil {
					t.Fatal(err)
				}
			}
			if mode == "expired" {
				now = oldExpiry
				if _, err := stage.BeginIntentRenewal(ctx, clock, nextExpiry); !errors.Is(err, ErrRevoked) {
					t.Fatal("expired renewal started", err)
				}
				return
			}
			renewal, err := stage.BeginIntentRenewal(ctx, clock, nextExpiry)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := stage.BeginIntentRenewal(ctx, clock, nextExpiry); !errors.Is(err, ErrConflict) {
				t.Fatal("overlapping renewal started", err)
			}
			if plan, done, err := stage.Advance(ctx, 1); done || plan.destination != "" || !errors.Is(err, ErrConflict) || stage.NextIndex() != prefix {
				t.Fatal("staging continued during renewal", done, err)
			}
			if _, err := stage.Checkpoint(); !errors.Is(err, ErrConflict) {
				t.Fatal("ambiguous checkpoint emitted", err)
			}
			if mode == "cancel" {
				cancelled, cancel := context.WithCancel(ctx)
				cancel()
				if done, err := renewal.Advance(cancelled, 3); done || !errors.Is(err, context.Canceled) || renewal.ExaminedScopes() != 0 {
					t.Fatal(done, err)
				}
				if done, err := renewal.Advance(ctx, 0); done || err == nil {
					t.Fatal("zero budget accepted", done, err)
				}
			}
			if mode == "drop" {
				renewal.operation.protocol.Port = &renewalDropPort{Port: m, token: stage.prepared.publication.Token, drop: true}
			}
			if mode == "lost" {
				m.blobAfter = func(_ string, f Fence) error {
					if f.Owner == stage.prepared.publication.Token && f.Intents[f.Owner].Expires.Equal(nextExpiry) {
						m.blobAfter = nil
						return lostReply
					}
					return nil
				}
			}
			if mode == "collector" {
				if _, err := p.SweepWithReaders(ctx, oldExpiry); err != nil {
					t.Fatal(err)
				}
			}
			if mode == "append" {
				appendOne(t, m, p, "owner", []byte("raced"), nil)
			}
			advance := func() (bool, error) {
				for {
					prior := renewal.ExaminedScopes()
					done, err := renewal.Advance(ctx, 3)
					if renewal.ExaminedScopes()-prior > 3 || stage.NextIndex() != prefix {
						t.Fatal("renewal exceeded budget or changed staging progress")
					}
					if err != nil || done {
						return done, err
					}
					if _, err := stage.Checkpoint(); !errors.Is(err, ErrConflict) {
						t.Fatal("partial renewal checkpoint emitted", err)
					}
					if !reflect.DeepEqual(original, m.roots["owner"]) {
						t.Fatal("renewal published a root")
					}
				}
			}
			done, err := advance()
			if mode == "drop" || mode == "lost" || mode == "collector" || mode == "append" {
				if err == nil || done {
					t.Fatal("failed renewal accepted", done, err)
				}
				if again, repeated := renewal.Advance(ctx, 3); again || repeated != err {
					t.Fatal("failed renewal reused", again, repeated)
				}
				if _, done, e := stage.Advance(ctx, 1); done || e != err {
					t.Fatal("failed stage reused", done, e)
				}
				if _, e := stage.Checkpoint(); !errors.Is(e, ErrConflict) {
					t.Fatal("failed renewal checkpoint emitted", e)
				}
				if mode == "collector" || mode == "append" {
					if _, e := p.ResumePrefixCompaction(ctx, saved); !errors.Is(e, ErrConflict) {
						t.Fatal("stale staging resumed", e)
					}
					return
				}
				stage, err = p.ResumePrefixCompaction(ctx, saved)
				if err != nil {
					t.Fatal(err)
				}
				renewal, err = stage.BeginIntentRenewal(ctx, clock, nextExpiry)
				if err != nil {
					t.Fatal(err)
				}
				done, err = advance()
			}
			if err != nil || !done || !stage.prepared.expires.Equal(nextExpiry) {
				t.Fatal("renewal incomplete", done, err)
			}
			updated := stageCheckpoint(t, stage)
			var descriptor compactionCheckpoint
			if err := json.Unmarshal(updated, &descriptor); err != nil || !descriptor.Expires.Equal(nextExpiry) || descriptor.Next != prefix {
				t.Fatal("checkpoint lost renewed expiry", err)
			}
			if mode == "resume-after" {
				stage, err = p.ResumePrefixCompaction(ctx, updated)
				if err != nil {
					t.Fatal(err)
				}
			}
			if mode == "repeated" {
				if _, done, err := stage.Advance(ctx, 2); done || err != nil {
					t.Fatal(done, err)
				}
				prefix += 2
				now = oldExpiry.Add(30 * time.Second)
				nextExpiry = epoch.Add(5 * time.Minute)
				renewal, err = stage.BeginIntentRenewal(ctx, clock, nextExpiry)
				if err != nil {
					t.Fatal(err)
				}
				if done, err := advance(); !done || err != nil {
					t.Fatal(done, err)
				}
			}
			now = oldExpiry.Add(time.Second)
			if _, err := p.SweepWithReaders(ctx, now); err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(original, m.roots["owner"]) {
				t.Fatal("expired staging fenced source")
			}
			var plan PreparedCompaction
			for {
				var done bool
				plan, done, err = stage.Advance(ctx, 2)
				if err != nil {
					t.Fatal("renewed staging failed", err)
				}
				if done {
					break
				}
			}
			if !plan.expires.Equal(nextExpiry) || stage.NextIndex() != 12 {
				t.Fatal("new records used old expiry")
			}
			commit, err := p.BeginCompactionCommit(ctx, plan)
			if err != nil {
				t.Fatal(err)
			}
			for {
				_, done, err := commit.Advance(ctx, 2, 4)
				if err != nil {
					t.Fatal("independent verification failed", err)
				}
				if done {
					break
				}
			}
			if commit.NextIndex() != 12 || commit.VerifiedNodes() != 19 {
				t.Fatal("renewal certified incomplete content", commit.NextIndex(), commit.VerifiedNodes())
			}
			// A completed renewal cannot reset later staging or mutate the plan.
			if done, err := renewal.Advance(ctx, 3); !done || err != nil || stage.NextIndex() != 12 {
				t.Fatal(done, err)
			}
			t.Logf("STAGE_RENEWAL mode=%s prefix=%d examined=%d renewed=%d records=12 nodes=19 checkpoint_bytes=%d", mode, prefix, renewal.ExaminedScopes(), renewal.RenewedScopes(), len(updated))
		})
	}
}
