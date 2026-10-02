//go:build linux

package integration_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	"js-wf/assignment"
	"js-wf/lease"
	"js-wf/provision"
)

type matrixAutomaticWrite struct {
	Controller string    `json:"controller"`
	Partition  uint32    `json:"partition"`
	Owner      string    `json:"owner"`
	Expected   uint64    `json:"expected"`
	Revision   uint64    `json:"revision"`
	Before     time.Time `json:"before"`
	After      time.Time `json:"after"`
	Error      string    `json:"error"`
}

type matrixAutomaticPort struct {
	assignment.RebalancePort
	id      string
	observe func(matrixAutomaticWrite)
}

func (p matrixAutomaticPort) Assign(ctx context.Context, partition uint32, owner string, expected uint64) (uint64, error) {
	before := time.Now().UTC()
	revision, err := p.RebalancePort.Assign(ctx, partition, owner, expected)
	event := matrixAutomaticWrite{Controller: p.id, Partition: partition, Owner: owner, Expected: expected, Revision: revision, Before: before, After: time.Now().UTC()}
	if err != nil {
		event.Error = err.Error()
	}
	p.observe(event)
	return revision, err
}

type matrixAutomaticAssignment struct {
	Partition uint32 `json:"partition"`
	Owner     string `json:"owner"`
	Revision  uint64 `json:"revision"`
}

type matrixAutomaticSnapshot struct {
	Observed            time.Time                   `json:"observed"`
	Members             []string                    `json:"members"`
	Assignments         []matrixAutomaticAssignment `json:"assignments"`
	Coordinator         lease.Value                 `json:"coordinator"`
	CoordinatorRevision uint64                      `json:"coordinator_revision"`
	MembershipStream    *jetstream.StreamInfo       `json:"membership_stream"`
}

func saveMatrixAutomaticSnapshot(ctx context.Context, js jetstream.JetStream, members *assignment.Membership, owners *assignment.Store, path string) error {
	bound, cancel := context.WithTimeout(ctx, 45*time.Second)
	defer cancel()
	var last string
	for bound.Err() == nil {
		attempt, stop := context.WithTimeout(bound, 3*time.Second)
		proof, err := readMatrixAutomaticSnapshot(attempt, js, members, owners)
		stop()
		if err != nil && !matrixTransientTransport(err) && !errors.Is(err, jetstream.ErrKeyNotFound) {
			return err
		}
		if err == nil && matrixAutomaticSnapshotReady(proof) {
			data, err := json.MarshalIndent(proof, "", "  ")
			if err != nil {
				return err
			}
			return os.WriteFile(path, data, 0644)
		}
		last = fmt.Sprintf("snapshot=%+v err=%v", proof, err)
		select {
		case <-bound.Done():
		case <-time.After(50 * time.Millisecond):
		}
	}
	return fmt.Errorf("automatic assignments never ready: %s: %w", last, bound.Err())
}

func readMatrixAutomaticSnapshot(ctx context.Context, js jetstream.JetStream, members *assignment.Membership, owners *assignment.Store) (proof matrixAutomaticSnapshot, err error) {
	proof.Members, err = members.Live(ctx)
	if err != nil {
		return proof, err
	}
	for part := uint32(0); part < provision.Partitions; part++ {
		owner, revision, err := owners.GetLatest(ctx, part)
		if err != nil {
			return proof, err
		}
		proof.Assignments = append(proof.Assignments, matrixAutomaticAssignment{part, owner, revision})
	}
	kv, err := js.KeyValue(ctx, assignment.MembershipBucket)
	if err != nil {
		return proof, err
	}
	coordinator, err := kv.Get(ctx, "coordinator.assign")
	if err != nil {
		return proof, err
	}
	if err := json.Unmarshal(coordinator.Value(), &proof.Coordinator); err != nil {
		return proof, err
	}
	proof.CoordinatorRevision = coordinator.Revision()
	stream, err := js.Stream(ctx, "KV_"+assignment.MembershipBucket)
	if err != nil {
		return proof, err
	}
	proof.MembershipStream, err = stream.Info(ctx)
	proof.Observed = time.Now().UTC()
	return proof, err
}

func matrixAutomaticSnapshotReady(proof matrixAutomaticSnapshot) bool {
	if len(proof.Members) != 5 || len(proof.Assignments) != int(provision.Partitions) || proof.Coordinator.Epoch == 0 || proof.CoordinatorRevision == 0 {
		return false
	}
	info := proof.MembershipStream
	if info == nil || info.Config.Replicas != 5 || info.Cluster == nil || info.Cluster.Leader == "" || len(info.Cluster.Replicas) != 4 {
		return false
	}
	for _, replica := range info.Cluster.Replicas {
		if replica == nil || !replica.Current || replica.Offline {
			return false
		}
	}
	counts := map[string]int{}
	for _, own := range proof.Assignments {
		if own.Revision == 0 {
			return false
		}
		counts[own.Owner]++
	}
	coordinatorLive := false
	for node, id := range proof.Members {
		if id != fmt.Sprintf("tier3-mixed-%d", node) || counts[id] != automaticPartitionQuota(node) {
			return false
		}
		coordinatorLive = coordinatorLive || proof.Coordinator.Worker == id
	}
	return coordinatorLive
}

func automaticPartitionQuota(node int) int {
	quota := int(provision.Partitions) / 5
	if node < int(provision.Partitions)%5 {
		quota++
	}
	return quota
}
