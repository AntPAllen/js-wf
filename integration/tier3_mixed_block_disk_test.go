//go:build linux

package integration_test

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"js-wf/testcluster"
)

func stallFiveContainerMixedBlockDisk(ctx context.Context, nc *nats.Conn, js jetstream.JetStream, cluster *testcluster.DockerCluster, disk *testcluster.BlockDisk, scheduled time.Time, prefix string, fault int) (matrixLeaderFault, error) {
	event := matrixLeaderFault{Scheduled: scheduled, Node: 4}
	bound, cancel := context.WithTimeout(ctx, 60*time.Second)
	defer cancel()
	binding, err := cluster.StoreBinding(bound, 4)
	if err != nil {
		return event, err
	}
	data, err := json.MarshalIndent(struct {
		Binding  testcluster.DockerStoreBinding `json:"binding"`
		StoreDir string                         `json:"store_dir"`
		Observed time.Time                      `json:"observed"`
	}{binding, disk.StoreDir, time.Now().UTC()}, "", "  ")
	if err == nil {
		err = os.WriteFile(prefix+"-block-disk-binding.json", data, 0644)
	}
	if err != nil {
		return event, err
	}
	// Both journal and dispatch leaders must be on the filesystem we stall.
	// This helper elects node four independently of any clock modification.
	roles, err := preferMatrixClockLeaders(bound, nc, js, cluster, "before", fault)
	if err != nil {
		return event, err
	}
	data, err = json.MarshalIndent(roles, "", "  ")
	if err == nil {
		err = os.WriteFile(prefix+"-block-disk-roles.json", data, 0644)
	}
	if err != nil {
		return event, err
	}
	proof, err := disk.Stall(bound, 5*time.Second)
	event.BlockStall = &proof
	event.Killed = proof.Suspended
	if err != nil {
		return event, err
	}
	if proof.Resumed.Sub(proof.Suspended) < 5*time.Second || proof.SyncReturned.Before(proof.Resumed) {
		return event, fmt.Errorf("unverified five-second block stall: %+v", proof)
	}
	if err := waitFiveReplicaReadiness(bound, js, 0); err != nil {
		return event, err
	}
	event.Healed = time.Now().UTC()
	return event, nil
}
