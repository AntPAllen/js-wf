package reconcile

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	"js-wf/internal/graphpublication"
)

var admissionStorageTouched = errors.New("admission storage touched")

type readerAdmissionJS struct {
	jetstream.JetStream
	calls int
}

func (p *readerAdmissionJS) KeyValue(context.Context, string) (jetstream.KeyValue, error) {
	p.calls++
	return nil, admissionStorageTouched
}

type readerAdmissionScope struct {
	graphpublication.Port
	scope string
}

func (p readerAdmissionScope) ReaderMaintenanceScope() string { return p.scope }

type readerAdmissionScan struct{ readerAdmissionScope }

func (readerAdmissionScan) NextRoot(context.Context, uint64) (*graphpublication.RootCatalogEntry, error) {
	return nil, nil
}

type readerAdmissionComplete struct{ readerAdmissionScan }

func (readerAdmissionComplete) RootCatalogHighWater(context.Context) (uint64, error) { return 0, nil }

func TestNativeReaderExpiryAdmissionPrecedesStorage(t *testing.T) {
	valid := readerAdmissionComplete{readerAdmissionScan{readerAdmissionScope{scope: strings.Repeat("a", 64)}}}
	for _, tc := range []struct {
		name     string
		port     graphpublication.Port
		id       string
		interval time.Duration
		budget   int
	}{
		{"no-port", nil, "worker", time.Second, 1},
		{"no-scan", readerAdmissionScope{scope: strings.Repeat("a", 64)}, "worker", time.Second, 1},
		{"no-watermark", valid.readerAdmissionScan, "worker", time.Second, 1},
		{"empty-scope", readerAdmissionComplete{readerAdmissionScan{readerAdmissionScope{}}}, "worker", time.Second, 1},
		{"invalid-scope", readerAdmissionComplete{readerAdmissionScan{readerAdmissionScope{scope: strings.Repeat("A", 64)}}}, "worker", time.Second, 1},
		{"empty-id", valid, "", time.Second, 1},
		{"invalid-budget", valid, "worker", time.Second, graphpublication.MaxReaderSweepBatch + 1},
		{"invalid-cadence", valid, "worker", 0, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			js := &readerAdmissionJS{}
			err := RunGraphReaderExpiry(context.Background(), js, graphpublication.Protocol{Port: tc.port}, tc.id, tc.interval, tc.budget)
			if err == nil || js.calls != 0 {
				t.Fatal(err, js.calls)
			}
		})
	}
	js := &readerAdmissionJS{}
	err := RunGraphReaderExpiry(context.Background(), js, graphpublication.Protocol{Port: valid}, "worker.with-punctuation", time.Second, 1)
	if !errors.Is(err, admissionStorageTouched) || js.calls != 1 {
		t.Fatal("valid configuration did not reach preparation", err, js.calls)
	}
	if err := RunGraphReaderExpiry(context.Background(), nil, graphpublication.Protocol{Port: valid}, "worker", time.Second, 1); err == nil {
		t.Fatal("nil JetStream accepted")
	}
}
