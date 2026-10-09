package graphpublication

import (
	"context"
	"errors"
	"testing"
	"time"

	"js-wf/internal/retainedgraph"
)

type rangeAuthorityFault struct {
	*memoryPort
	fail bool
}

func (p *rangeAuthorityFault) ReadRoot(c context.Context, key string) (Root, error) {
	if p.fail {
		return Root{}, errors.New("authority unavailable")
	}
	return p.memoryPort.ReadRoot(c, key)
}

func TestGraphReaderRangeRevokedBetweenLeaves(t *testing.T) {
	for _, mode := range []string{"release", "expiry", "unknown", "changed-snapshot"} {
		t.Run(mode, func(t *testing.T) {
			m, p, root, old := readerFixture(t)
			port := &rangeAuthorityFault{memoryPort: m}
			p.Port = port
			prepared, err := p.PrepareAppend(ctx, "history", root.Head, []byte("second"), nil, epoch.Add(time.Hour))
			if err != nil {
				t.Fatal(err)
			}
			root, err = p.Commit(ctx, prepared)
			if err != nil {
				t.Fatal(err)
			}
			_, err = p.ReleaseReader(ctx, old, root.Head)
			if err != nil {
				t.Fatal(err)
			}
			root = m.roots["history"]
			reader, root, err := p.AcquireReader(ctx, "history", root.Head, epoch.Add(2*time.Hour))
			if err != nil {
				t.Fatal(err)
			}
			now, visits := epoch.Add(time.Hour), 0
			err = p.ReadRetainedRange(ctx, reader, "", 0, 2, func() time.Time { return now }, func(uint64, retainedgraph.Record) error {
				visits++
				switch mode {
				case "release":
					_, e := p.ReleaseReader(ctx, reader, root.Head)
					return e
				case "expiry":
					now = epoch.Add(2 * time.Hour)
				case "unknown":
					port.fail = true
				case "changed-snapshot":
					r := m.roots["history"]
					r.Readers[0].Graph = retainedgraph.Empty()
					m.roots["history"] = r
				}
				return nil
			})
			if err == nil || visits != 1 {
				t.Fatal("read continued past authority loss", visits, err)
			}
			if mode != "unknown" && !errors.Is(err, ErrRevoked) {
				t.Fatal(err)
			}
		})
	}
}
