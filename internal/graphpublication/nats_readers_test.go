package graphpublication

import (
	"errors"
	"fmt"
	"reflect"
	"testing"
	"time"

	"js-wf/internal/retainedgraph"
)

func TestNativeGraphReadersRetirementAndExpiry(t *testing.T) {
	for _, replicas := range []int{1, 3} {
		t.Run(fmt.Sprintf("R%d", replicas), func(t *testing.T) {
			port, c := nativeGraphObjectFixture(t, replicas)
			p := Protocol{Port: port}
			now := time.Now().UTC()
			prepared, err := p.PrepareAppend(c, "history", 0, []byte("retained"), [][]byte{[]byte("shared-native")}, now.Add(time.Hour))
			if err != nil {
				t.Fatal(err)
			}
			root, err := p.Commit(c, prepared)
			if err != nil {
				t.Fatal(err)
			}
			reader, root, err := p.AcquireReader(c, "history", root.Head, now.Add(2*time.Hour))
			if err != nil {
				t.Fatal(err)
			}
			original, err := p.ReadRetained(c, reader, 0, now)
			if err != nil {
				t.Fatal(err)
			}
			prepared, err = p.PrepareAppendWithOwned(c, "history", root.Head, []byte("later"), nil, []OwnedPayload{{Index: 0, Link: original.Blobs[0]}}, now.Add(time.Hour))
			if err != nil {
				t.Fatal(err)
			}
			root, err = p.Commit(c, prepared)
			if err != nil {
				t.Fatal(err)
			}
			if err = p.Retire(c, "history", root.Head); err == nil {
				t.Fatal("v1 retirement downgraded schema")
			}
			if err = p.RetireLive(c, "history", root.Head); err != nil {
				t.Fatal(err)
			}
			if _, err = p.Sweep(c, now.Add(time.Hour)); err != nil {
				t.Fatal(err)
			}
			retired, err := port.ReadRoot(c, "history")
			if err != nil || retired.Graph.Count != 0 || retired.Schema != RetentionSchema || len(retired.Readers) != 1 {
				t.Fatal(retired, err)
			}
			// Reopen the same isolated authority/object adapter; no graph bytes or pins
			// are loaded into process-local protection maps.
			reopened, err := OpenNativePort(c, port.NativeAuthority, port.bucket)
			if err != nil {
				t.Fatal(err)
			}
			p.Port = reopened
			got, err := p.ReadRetained(c, reader, 0, now.Add(time.Hour))
			if err != nil || !reflect.DeepEqual(got, original) {
				t.Fatal(got, err)
			}
			payload, err := reopened.Get(c, got.Blobs[0], 100)
			if err != nil || string(payload) != "shared-native" {
				t.Fatal(string(payload), err)
			}
			retired, err = p.RenewReader(c, reader, retired.Head, now.Add(4*time.Hour))
			if err != nil {
				t.Fatal(err)
			}
			if _, err = p.Sweep(c, now.Add(3*time.Hour)); err != nil {
				t.Fatal(err)
			}
			if _, err = retainedgraph.Read(c, nativeGraphReadStore{reopened}, reader.Snapshot(), 0); err != nil {
				t.Fatal(err)
			}
			if _, err = p.Sweep(c, now.Add(4*time.Hour)); err != nil {
				t.Fatal(err)
			}
			final, err := reopened.ReadRoot(c, "history")
			if err != nil || final.Head != retired.Head+1 || len(final.Readers) != 0 || final.Schema != RetentionSchema {
				t.Fatal(final, err)
			}
			if _, err = p.RenewReader(c, reader, final.Head, now.Add(5*time.Hour)); !errors.Is(err, ErrRevoked) {
				t.Fatal(err)
			}
			if _, err = reopened.CASRoot(c, "history", final.Head, EmptyRoot()); err == nil {
				t.Fatal("downgrade after expiry")
			}
			nativeGraphNoObjects(t, reopened, c)
		})
	}
}
