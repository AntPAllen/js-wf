//go:build linux

package integration_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	"js-wf/testcluster"
)

// This adapter changes only an opt-in disposable cohort's transport. It never
// changes message bodies, delivery order, callbacks or the latency evaluator.
type matrixBulkCursorFault struct {
	jetstream.JetStream
	cluster *testcluster.DockerCluster
	root    string
	ctx     context.Context
	cancel  context.CancelFunc
	once    sync.Once
	mu      sync.Mutex
	proof   matrixBulkCursorFaultProof
}

type matrixBulkCursorFaultProof struct {
	TriggerSequence  uint64                            `json:"trigger_sequence"`
	Target           *jetstream.ConsumerInfo           `json:"target"`
	Kill             testcluster.DockerKillObservation `json:"kill"`
	RestartCompleted time.Time                         `json:"restart_completed"`
	Cursors          []*jetstream.ConsumerInfo         `json:"cursors"`
	Error            string                            `json:"error"`
}

func (f *matrixBulkCursorFault) Stream(ctx context.Context, name string) (jetstream.Stream, error) {
	s, err := f.JetStream.Stream(ctx, name)
	if err != nil || name != "WF_JRN" {
		return s, err
	}
	return matrixBulkFaultStream{Stream: s, fault: f}, nil
}

type matrixBulkFaultStream struct {
	jetstream.Stream
	fault *matrixBulkCursorFault
}

func (s matrixBulkFaultStream) CreateConsumer(ctx context.Context, cfg jetstream.ConsumerConfig) (jetstream.Consumer, error) {
	c, err := s.Stream.CreateConsumer(ctx, cfg)
	if err != nil {
		return nil, err
	}
	// Observe actual public metadata without replacing the scanner's consumer.
	call, stop := context.WithTimeout(ctx, 2*time.Second)
	info, err := c.Info(call)
	stop()
	if err != nil {
		// Retain the consumer so the scanner can execute its normal cleanup.
		s.fault.fail(err)
		return c, nil
	}
	s.fault.mu.Lock()
	s.fault.proof.Cursors = append(s.fault.proof.Cursors, info)
	s.fault.mu.Unlock()
	return matrixBulkFaultConsumer{Consumer: c, fault: s.fault}, nil
}

type matrixBulkFaultConsumer struct {
	jetstream.Consumer
	fault *matrixBulkCursorFault
}

func (c matrixBulkFaultConsumer) Consume(handler jetstream.MessageHandler, opts ...jetstream.PullConsumeOpt) (jetstream.ConsumeContext, error) {
	return c.Consumer.Consume(func(msg jetstream.Msg) {
		meta, err := msg.Metadata()
		if err != nil {
			c.fault.fail(err)
		} else if meta.Sequence.Stream >= 128 {
			c.fault.once.Do(func() {
				if err := c.fault.inject(c.Consumer, meta.Sequence.Stream); err != nil {
					c.fault.fail(err)
				}
			})
		}
		handler(msg)
	}, opts...)
}

func (f *matrixBulkCursorFault) fail(err error) {
	f.mu.Lock()
	if f.proof.Error == "" {
		f.proof.Error = err.Error()
	}
	f.mu.Unlock()
	f.cancel()
}

func (f *matrixBulkCursorFault) inject(c jetstream.Consumer, sequence uint64) error {
	call, stop := context.WithTimeout(f.ctx, 2*time.Second)
	info, err := c.Info(call)
	stop()
	if err != nil {
		return err
	}
	if info.Stream != "WF_JRN" || info.Config.Replicas != 1 || !info.Config.MemoryStorage || info.Config.AckPolicy != jetstream.AckNonePolicy || info.Cluster == nil || info.NumPending == 0 {
		return fmt.Errorf("bulk fault lacks active R1 memory AckNone cursor: %+v", info)
	}
	owner := -1
	for n := 0; n < 5; n++ {
		if f.cluster.NodeName(n) == info.Cluster.Leader {
			owner = n
		}
	}
	if owner < 0 {
		return fmt.Errorf("bulk cursor owner %q absent", info.Cluster.Leader)
	}
	f.mu.Lock()
	f.proof.TriggerSequence, f.proof.Target = sequence, info
	f.mu.Unlock()
	logs, err := f.cluster.Logs(owner)
	if err != nil {
		return err
	}
	if err = os.WriteFile(filepath.Join(f.root, "bulk-owner-before-kill.log"), []byte(logs), 0600); err != nil {
		return err
	}
	kill, err := f.cluster.KillNodeObserved(owner)
	f.mu.Lock()
	f.proof.Kill = kill
	f.mu.Unlock()
	if err != nil {
		return err
	}
	if kill.SourceStopped.IsZero() {
		return fmt.Errorf("bulk cursor owner source exit unconfirmed")
	}
	if err = f.cluster.RestartNode(owner); err != nil {
		return err
	}
	f.mu.Lock()
	f.proof.RestartCompleted = time.Now().UTC()
	f.mu.Unlock()
	return nil
}

func (f *matrixBulkCursorFault) snapshot() matrixBulkCursorFaultProof {
	f.mu.Lock()
	defer f.mu.Unlock()
	p := f.proof
	p.Cursors = append([]*jetstream.ConsumerInfo(nil), p.Cursors...)
	return p
}

// Fresh restored stores can contain historical audit cursors. Prepare only the
// supplied disposable copy before the source cut, retaining every public name,
// configuration and deletion receipt. Workflow records are not altered.
func matrixPrepareCopiedBulkCursors(ctx context.Context, js jetstream.JetStream, root string) (failure error) {
	type removed struct {
		Info       *jetstream.ConsumerInfo `json:"info"`
		AttemptUTC time.Time               `json:"attempt_utc"`
		DeletedUTC time.Time               `json:"deleted_utc"`
		Error      string                  `json:"error"`
	}
	var receipts []removed
	defer func() {
		data, err := json.MarshalIndent(struct {
			Receipts []removed `json:"receipts"`
			Error    string    `json:"error"`
		}{receipts, fmt.Sprint(failure)}, "", "  ")
		if err == nil {
			err = os.WriteFile(filepath.Join(root, "bulk-cursor-preparation.json"), data, 0600)
		}
		failure = errors.Join(failure, err)
	}()
	for _, name := range []string{"WF_INV", "WF_JRN", "KV_WF_STATE", "WF_SIG"} {
		s, err := js.Stream(ctx, name)
		if err != nil {
			return err
		}
		list := s.ListConsumers(ctx)
		for info := range list.Info() {
			if !strings.HasPrefix(info.Name, "wf-audit-") || info.Config.AckPolicy != jetstream.AckNonePolicy || !info.Config.MemoryStorage {
				return fmt.Errorf("unexpected copied source consumer %s/%s", name, info.Name)
			}
			r := removed{Info: info, AttemptUTC: time.Now().UTC()}
			if err := s.DeleteConsumer(ctx, info.Name); err != nil {
				r.Error = err.Error()
				receipts = append(receipts, r)
				return err
			}
			r.DeletedUTC = time.Now().UTC()
			receipts = append(receipts, r)
		}
		if err := list.Err(); err != nil {
			return err
		}
		info, err := s.Info(ctx)
		if err != nil || info.State.Consumers != 0 {
			return fmt.Errorf("copied bulk preparation %s not empty: %v", name, err)
		}
	}
	return nil
}
