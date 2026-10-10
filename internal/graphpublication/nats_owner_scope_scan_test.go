package graphpublication

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/nats-io/nats.go/jetstream"
)

type ownerScanCountStream struct {
	jetstream.Stream
	calls int
	lost  bool
}

func (s *ownerScanCountStream) GetMsg(c context.Context, seq uint64, opts ...jetstream.GetMsgOpt) (*jetstream.RawStreamMsg, error) {
	s.calls++
	if s.lost {
		return nil, lostReply
	}
	return s.Stream.GetMsg(c, seq, opts...)
}

func TestNativeGraphOwnerScanSequenceChurnCompleteness(t *testing.T) {
	for _, mode := range []string{"healthy-churn", "undiscovered-churn", "barrier-churn", "malformed", "lost-read"} {
		t.Run(mode, func(t *testing.T) {
			_, legacy, c := nativeGraphFixture(t, 1)
			js := legacy.js
			if _, err := js.CreateStream(c, OwnerIndexedAuthorityStreamConfig("SCAN_INDEX", "wf.scan", 1)); err != nil {
				t.Fatal(err)
			}
			if _, err := js.CreateStream(c, NativeObjectStreamConfig("SCAN_OBJECTS", 1)); err != nil {
				t.Fatal(err)
			}
			a, err := OpenOwnerIndexedNativeAuthority(c, js, "SCAN_INDEX", "wf.scan")
			if err != nil {
				t.Fatal(err)
			}
			port, err := OpenOwnerIndexedNativePort(c, a, "SCAN_OBJECTS")
			if err != nil {
				t.Fatal(err)
			}
			keys := []string{}
			for i := 0; i < 3; i++ {
				k := authorityKey(key([]byte(fmt.Sprint(i))), "owner")
				keys = append(keys, k)
				if err = a.ensureOwnerScope(c, "owner", k); err != nil {
					t.Fatal(err)
				}
			}
			if mode == "malformed" {
				if _, err = js.Publish(c, a.ownerSubject("owner", keys[1]), []byte(`{"bad":true}`)); err != nil {
					t.Fatal(err)
				}
			}
			scan, err := port.BeginOwnerScopeScan(c, "owner")
			if err != nil {
				t.Fatal(err)
			}
			stream := &ownerScanCountStream{Stream: a.stream}
			a.stream = stream
			first, done, err := scan.Advance(c, 1)
			if err != nil || done || len(first) != 1 || first[0] != keys[0] {
				t.Fatal("first scope", first, done, err)
			}
			switch mode {
			case "healthy-churn":
				if err = port.ValidateOwnerScope(c, "owner", keys[0]); err != nil {
					t.Fatal(err)
				}
			case "undiscovered-churn":
				if err = port.ValidateOwnerScope(c, "owner", keys[1]); err != nil {
					t.Fatal(err)
				}
			case "barrier-churn":
				if err = port.ValidateOwnerScope(c, "owner", ownerScanBarrierKey("owner")); err != nil {
					t.Fatal(err)
				}
			case "lost-read":
				stream.lost = true
			}
			seen := map[string]bool{first[0]: true}
			for calls := 0; calls < 5 && !done && err == nil; calls++ {
				var batch []string
				batch, done, err = scan.Advance(c, 1)
				if err != nil && len(batch) != 0 {
					t.Fatal("partial error keys escaped", batch, err)
				}
				for _, k := range batch {
					if seen[k] {
						t.Fatal("duplicate native key", k)
					}
					seen[k] = true
				}
			}
			if mode == "healthy-churn" {
				if err != nil || !done || len(seen) != 4 || !seen[ownerScanBarrierKey("owner")] {
					t.Fatal("healthy churn incomplete", seen, done, err)
				}
			} else {
				if err == nil || done {
					t.Fatal("incomplete scan certified", seen, done, err)
				}
				before := stream.calls
				if batch, done, e := scan.Advance(c, 1); batch != nil || done || e != err || stream.calls != before {
					t.Fatal("failed scan retried", batch, done, e)
				}
				if mode == "lost-read" && !errors.Is(err, lostReply) {
					t.Fatal(err)
				}
			}
			t.Logf("NATIVE_OWNER_SCAN mode=%s seen=%d expected=4 complete=%t err=%v", mode, len(seen), done, err)
		})
	}
}
