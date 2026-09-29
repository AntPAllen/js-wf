package sim

import (
	"context"
	"fmt"
	"time"

	"js-wf/journal"
)

// SnapshotActor runs a production compaction decision with a private Store
// over cooperative calls to the same retained transports.
type SnapshotActor struct {
	Name string
	Run  func(context.Context, *journal.Store) error
}

type yieldingSnapshotReadPort struct {
	yield     YieldFunc
	transport journal.ReadPort
}

func (p yieldingSnapshotReadPort) Next(ctx context.Context, subject string, from uint64) (journal.AppendTail, error) {
	var value journal.AppendTail
	var opErr error
	if err := p.yield(ctx, "next_journal", func() { value, opErr = p.transport.Next(ctx, subject, from) }); err != nil {
		return journal.AppendTail{}, err
	}
	return value, opErr
}
func (p yieldingSnapshotReadPort) Wait(ctx context.Context, delay time.Duration) error {
	var opErr error
	if err := p.yield(ctx, "journal_wait", func() { opErr = p.transport.Wait(ctx, delay) }); err != nil {
		return err
	}
	return opErr
}

type yieldingSnapshotPort struct {
	yield     YieldFunc
	transport journal.SnapshotWritePort
}

var _ journal.SnapshotWritePort = yieldingSnapshotPort{}

func (p yieldingSnapshotPort) do(ctx context.Context, operation string, fn func() error) error {
	var opErr error
	if err := p.yield(ctx, operation, func() { opErr = fn() }); err != nil {
		return err
	}
	return opErr
}
func (p yieldingSnapshotPort) GetManifest(ctx context.Context, key string) ([]byte, error) {
	var value []byte
	err := p.do(ctx, "get_manifest", func() error { var e error; value, e = p.transport.GetManifest(ctx, key); return e })
	return value, err
}
func (p yieldingSnapshotPort) GetManifestRevision(ctx context.Context, key string) (journal.SnapshotManifestValue, error) {
	var value journal.SnapshotManifestValue
	err := p.do(ctx, "get_manifest_revision", func() error { var e error; value, e = p.transport.GetManifestRevision(ctx, key); return e })
	return value, err
}
func (p yieldingSnapshotPort) GetObject(ctx context.Context, name string) ([]byte, error) {
	var value []byte
	err := p.do(ctx, "get_snapshot_object", func() error { var e error; value, e = p.transport.GetObject(ctx, name); return e })
	return value, err
}
func (p yieldingSnapshotPort) PutObject(ctx context.Context, name string, data []byte) error {
	payload := append([]byte(nil), data...)
	return p.do(ctx, "put_snapshot_object", func() error { return p.transport.PutObject(ctx, name, payload) })
}
func (p yieldingSnapshotPort) CreateManifest(ctx context.Context, key string, data []byte) error {
	payload := append([]byte(nil), data...)
	return p.do(ctx, "create_manifest", func() error { return p.transport.CreateManifest(ctx, key, payload) })
}
func (p yieldingSnapshotPort) UpdateManifest(ctx context.Context, key string, data []byte, revision uint64) error {
	payload := append([]byte(nil), data...)
	return p.do(ctx, "update_manifest", func() error { return p.transport.UpdateManifest(ctx, key, payload, revision) })
}
func (p yieldingSnapshotPort) PurgeJournal(ctx context.Context, subject string, before uint64) error {
	return p.do(ctx, "purge_journal", func() error { return p.transport.PurgeJournal(ctx, subject, before) })
}
func (p yieldingSnapshotPort) PurgeSignals(ctx context.Context, subject string, before uint64) error {
	return p.do(ctx, "purge_signals", func() error { return p.transport.PurgeSignals(ctx, subject, before) })
}
func (p yieldingSnapshotPort) Wait(ctx context.Context, delay time.Duration) error {
	waiter, ok := p.transport.(interface {
		Wait(context.Context, time.Duration) error
	})
	if !ok {
		return fmt.Errorf("snapshot transport has no wait")
	}
	return p.do(ctx, "snapshot_wait", func() error { return waiter.Wait(ctx, delay) })
}

func RunSnapshotActors(ctx context.Context, schedule *Scheduler, appendPort journal.AppendPort, readPort journal.ReadPort, snapshotPort journal.SnapshotWritePort, actors []SnapshotActor) (map[string]error, error) {
	if appendPort == nil || readPort == nil || snapshotPort == nil {
		return nil, fmt.Errorf("snapshot actor transport missing")
	}
	cooperative := make([]CooperativeActor, 0, len(actors))
	for _, actor := range actors {
		actor := actor
		if actor.Run == nil {
			return nil, fmt.Errorf("snapshot actor %q has no run function", actor.Name)
		}
		cooperative = append(cooperative, CooperativeActor{Name: actor.Name, Run: func(ctx context.Context, yield YieldFunc) error {
			return actor.Run(ctx, journal.NewWithSnapshotPort(yieldingAppendPort{yield: yield, transport: appendPort}, yieldingSnapshotReadPort{yield: yield, transport: readPort}, yieldingSnapshotPort{yield: yield, transport: snapshotPort}))
		}})
	}
	return RunCooperative(ctx, schedule, cooperative)
}
