package worker

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

// Keep the production lease adapter and decisions; observe its native KV calls.
type sdkLeaseTraceKV struct {
	jetstream.KeyValue
	t  *testing.T
	js jetstream.JetStream
}

func (p sdkLeaseTraceKV) Create(ctx context.Context, key string, value []byte, opts ...jetstream.KVCreateOpt) (uint64, error) {
	start := time.Now()
	rev, err := p.KeyValue.Create(ctx, key, value, opts...)
	p.t.Logf("SDK_LEASE_CREATE key=%s revision=%d value=%s elapsed=%s error=%q", key, rev, value, time.Since(start), err)
	if err != nil {
		p.capture(key)
	}
	return rev, err
}

func (p sdkLeaseTraceKV) Update(ctx context.Context, key string, value []byte, expected uint64) (uint64, error) {
	start := time.Now()
	rev, err := p.KeyValue.Update(ctx, key, value, expected)
	p.t.Logf("SDK_LEASE_UPDATE key=%s expected=%d revision=%d value=%s elapsed=%s error=%q", key, expected, rev, value, time.Since(start), err)
	if err != nil {
		p.capture(key)
	}
	return rev, err
}

func (p sdkLeaseTraceKV) capture(key string) {
	// A fresh bounded context observes the failure even if the delivery expired.
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	stream, err := p.js.Stream(ctx, "KV_WF_LEASE")
	if err != nil {
		p.t.Logf("SDK_LEASE_FAILURE stream_error=%q", err)
		return
	}
	info, infoErr := stream.Info(ctx)
	raw, rawErr := stream.GetLastMsgForSubject(ctx, "$KV.WF_LEASE."+key)
	proof, _ := json.Marshal(struct {
		Info      *jetstream.StreamInfo   `json:"info"`
		Raw       *jetstream.RawStreamMsg `json:"raw"`
		InfoError string                  `json:"info_error"`
		RawError  string                  `json:"raw_error"`
	}{Info: info, Raw: raw, InfoError: sdkLeaseError(infoErr), RawError: sdkLeaseError(rawErr)})
	p.t.Logf("SDK_LEASE_FAILURE key=%s proof=%s", key, proof)
}

func sdkLeaseError(err error) string {
	if err != nil {
		return err.Error()
	}
	return ""
}
