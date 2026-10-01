package journal

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

type checkpointManifestProbe struct {
	SnapshotReadPort
	value          []byte
	err            error
	calls, objects int
}

func (p *checkpointManifestProbe) GetManifest(context.Context, string) ([]byte, error) {
	p.calls++
	return p.value, p.err
}
func (p *checkpointManifestProbe) GetObject(context.Context, string) ([]byte, error) {
	p.objects++
	return nil, fmt.Errorf("archive access forbidden")
}
func TestCheckpointReadAbsentOrdinaryAndMalformedMetadata(t *testing.T) {
	hash := hex.EncodeToString(make([]byte, 32))
	key := sha256.Sum256([]byte("test.probe"))
	ordinary, _ := json.Marshal(Snapshot{Version: 1, LastIndex: 3, LastSeq: 14, Epoch: 51, Object: "snapshot-" + hex.EncodeToString(key[:8]) + "-14-prefix", SHA256: hash})
	for _, tt := range []struct {
		name      string
		value     []byte
		err, want error
	}{
		{"absent", nil, jetstream.ErrKeyNotFound, nil},
		{"ordinary", ordinary, nil, nil},
		{"unknown-version", []byte(`{"version":3}`), nil, ErrGap},
		{"missing-runtime", []byte(`{"version":2,"object":"snapshot-invalid"}`), nil, ErrGap},
		{"malformed", []byte(`{`), nil, ErrGap},
	} {
		t.Run(tt.name, func(t *testing.T) {
			port := &checkpointManifestProbe{value: tt.value, err: tt.err}
			view, err := NewWithSnapshotReadPort(nil, nil, port).ReadCheckpoint(context.Background(), "test", "probe", 17)
			if view != nil || !errors.Is(err, tt.want) || port.calls != 1 || port.objects != 0 {
				t.Fatalf("view=%+v err=%v calls=%d objects=%d", view, err, port.calls, port.objects)
			}
		})
	}
}

func TestCheckpointReadInvalidIdentityAndCancellationBeforeTransport(t *testing.T) {
	port := &checkpointManifestProbe{}
	store := NewWithSnapshotReadPort(nil, nil, port)
	if _, err := store.ReadCheckpoint(context.Background(), "test", "probe", 0); !errors.Is(err, ErrCheckpointGeneration) {
		t.Fatal(err)
	}
	if _, err := store.ReadCheckpoint(context.Background(), "invalid.type", "probe", 17); err == nil {
		t.Fatal("invalid identity accepted")
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	cancel()
	if _, err := store.ReadCheckpoint(ctx, "test", "probe", 17); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if port.calls != 0 || port.objects != 0 {
		t.Fatal("invalid call reached transport")
	}
}
