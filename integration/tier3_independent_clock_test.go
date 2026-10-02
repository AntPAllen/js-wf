//go:build linux

package integration_test

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	"js-wf/runtimeclock"
	"js-wf/testcluster"
)

type matrixIndependentClockProof struct {
	Config runtimeclock.Config     `json:"config"`
	Probes []*jetstream.StreamInfo `json:"probes"`
	Before time.Time               `json:"before"`
	Lower  time.Time               `json:"lower"`
	Upper  time.Time               `json:"upper"`
	After  time.Time               `json:"after"`
}

func prepareMatrixIndependentClock(ctx context.Context, js jetstream.JetStream, cluster *testcluster.DockerCluster, tags map[int][]string, root string) (*runtimeclock.Clock, error) {
	cfg := runtimeclock.DefaultConfig()
	for node := 0; node < 5; node++ {
		if len(tags[node]) != 1 {
			return nil, fmt.Errorf("independent clock requires a unique tag per node")
		}
		cfg.Probes = append(cfg.Probes, runtimeclock.Probe{Name: fmt.Sprintf("WF_CLOCK_%d", node), Server: cluster.NodeName(node), Identity: fmt.Sprintf("docker-node-%d", node), Tag: tags[node][0]})
	}
	ready, stop := context.WithTimeout(ctx, 45*time.Second)
	defer stop()
	var err error
	for ready.Err() == nil {
		err = runtimeclock.EnsureProbes(ready, js, cfg)
		if err == nil {
			break
		}
		if !matrixTransientTransport(err) {
			return nil, err
		}
		select {
		case <-ready.Done():
		case <-time.After(100 * time.Millisecond):
		}
	}
	if err != nil {
		return nil, err
	}
	clock, err := runtimeclock.NewClock(js, cfg)
	if err != nil {
		return nil, err
	}
	proof := matrixIndependentClockProof{Config: cfg, Before: time.Now().UTC()}
	proof.Lower, proof.Upper, err = clock.Bounds(ready)
	proof.After = time.Now().UTC()
	if err != nil {
		return nil, err
	}
	if proof.Lower.After(proof.After) || proof.Upper.Before(proof.Before) {
		return nil, fmt.Errorf("clock bounds do not overlap unshifted controller startup bracket")
	}
	for _, probe := range cfg.Probes {
		stream, err := js.Stream(ready, probe.Name)
		if err != nil {
			return nil, err
		}
		proof.Probes = append(proof.Probes, stream.CachedInfo())
	}
	data, err := json.MarshalIndent(proof, "", "  ")
	if err != nil {
		return nil, err
	}
	if err := os.WriteFile(filepath.Join(root, "independent-clock.json"), data, 0644); err != nil {
		return nil, err
	}
	return clock, nil
}
