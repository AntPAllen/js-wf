package graphpublication

import (
	"bytes"
	"fmt"
	"testing"
	"time"
)

func TestNativeGraphApplicationRetirement(t *testing.T) {
	for _, replicas := range []int{1, 3} {
		t.Run(fmt.Sprintf("R%d", replicas), func(t *testing.T) {
			port, c := nativeGraphObjectFixture(t, replicas)
			p := Protocol{Port: port}
			now := time.Now().UTC()
			app := []byte(`{"generation":1,"index":0,"epoch":2,"terminal":true}`)
			prepared, err := p.PrepareAppendWithApplication(c, "journal", 0, []byte("terminal"), [][]byte{[]byte("input")}, nil, now.Add(time.Hour), app)
			if err != nil {
				t.Fatal(err)
			}
			root, err := p.Commit(c, prepared)
			if err != nil {
				t.Fatal(err)
			}
			reader, root, err := p.AcquireReader(c, "journal", root.Head, now.Add(2*time.Hour))
			if err != nil || root.Schema != ApplicationSchema || !bytes.Equal(root.Application, app) {
				t.Fatal(root, err)
			}
			if err = p.Retire(c, "journal", root.Head); err == nil {
				t.Fatal("legacy retirement erased lifecycle")
			}
			if err = p.RetireLive(c, "journal", root.Head); err != nil {
				t.Fatal(err)
			}
			// Reopen adapters, retaining only committed native authority/object state.
			authority, err := OpenNativeAuthority(c, port.js, port.name, port.prefix)
			if err != nil {
				t.Fatal(err)
			}
			fresh, err := OpenNativePort(c, authority, port.bucket)
			if err != nil {
				t.Fatal(err)
			}
			p = Protocol{Port: fresh}
			root, err = fresh.ReadRoot(c, "journal")
			if err != nil || root.Graph.Count != 0 || !bytes.Equal(root.Application, app) {
				t.Fatal(root, err)
			}
			if _, err = p.ReadRetained(c, reader, 0, now); err != nil {
				t.Fatal(err)
			}
			if _, err = p.SweepWithReaders(c, now.Add(2*time.Hour)); err != nil {
				t.Fatal(err)
			}
			nativeGraphNoObjects(t, fresh, c)
			root, err = fresh.ReadRoot(c, "journal")
			if err != nil || len(root.Readers) != 0 || !bytes.Equal(root.Application, app) {
				t.Fatal(root, err)
			}
			root, err = p.UpdateApplication(c, "journal", root.Head, nil)
			if err != nil || root.Schema != ApplicationSchema {
				t.Fatal(root, err)
			}
			for _, schema := range []string{Schema, RetentionSchema} {
				next := root
				next.Schema = schema
				if _, err = fresh.CASRoot(c, "journal", root.Head, next); err == nil {
					t.Fatal("schema downgrade accepted", schema)
				}
			}
		})
	}
}
