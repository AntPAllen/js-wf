package main

import (
	"context"
	"fmt"
	"time"

	"js-wf/journal"
	"js-wf/provision"
	"js-wf/testcluster"

	"github.com/nats-io/nats.go/jetstream"
)

// These metadata requests occur outside the timed append loop. A before/after
// snapshot does not rule out a transient leader change between the snapshots.
func captureTopology(ctx context.Context, js jetstream.JetStream, clientServer string) (topology, error) {
	attempt, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	stream, err := js.Stream(attempt, "WF_JRN")
	if err != nil {
		return topology{}, err
	}
	info, err := stream.Info(attempt)
	if err != nil {
		return topology{}, err
	}
	if info.Cluster == nil || info.Cluster.Leader == "" || clientServer == "" {
		return topology{}, fmt.Errorf("missing journal leader or client server")
	}
	return topology{Leader: info.Cluster.Leader, ClientServer: clientServer, ClientIsLeader: info.Cluster.Leader == clientServer}, nil
}

type hotProbe struct {
	Round       int      `json:"round"`
	Node        int      `json:"node"`
	Before      topology `json:"before"`
	After       topology `json:"after"`
	Measurement result   `json:"measurement"`
}

type topologyProbeReport struct {
	Scope    string     `json:"scope"`
	Samples  []hotProbe `json:"samples"`
	Messages uint64     `json:"messages"`
	Subjects uint64     `json:"subjects"`
}

// Rotate the first node each round so placement is not confounded with a fixed
// measurement order. Each sample uses a fresh subject on the same cluster.
// This diagnostic preserves measurements; it never changes the release gate.
func runTopologyProbe(root string, entries int) (topologyProbeReport, error) {
	rep := topologyProbeReport{Scope: "diagnostic: pinned client placement on one three-replica file cluster"}
	c, err := testcluster.Start(root, 3)
	if err != nil {
		return rep, err
	}
	defer c.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	var all [3]jetstream.JetStream
	for i, nc := range c.Clients {
		all[i], err = jetstream.New(nc)
		if err != nil {
			return rep, err
		}
	}
	for until := time.Now().Add(30 * time.Second); ; {
		attempt, stop := context.WithTimeout(ctx, 3*time.Second)
		err = provision.Ensure(attempt, all[0], 3)
		stop()
		if err == nil {
			break
		}
		if time.Now().After(until) {
			return rep, err
		}
		time.Sleep(50 * time.Millisecond)
	}
	for round := 0; round < 3; round++ {
		for offset := 0; offset < 3; offset++ {
			node := (round + offset) % 3
			sample := hotProbe{Round: round + 1, Node: node}
			sample.Before, err = captureTopology(ctx, all[node], c.Clients[node].ConnectedServerName())
			if err != nil {
				return rep, err
			}
			sample.Measurement, err = appendJournal(ctx, journal.New(all[node]), all[node], fmt.Sprintf("probe-%d-%d", round, node), entries)
			if err != nil {
				return rep, err
			}
			sample.After, err = captureTopology(ctx, all[node], c.Clients[node].ConnectedServerName())
			if err != nil {
				return rep, err
			}
			rep.Samples = append(rep.Samples, sample)
			fmt.Printf("probe round=%d node=%d client=%s leader=%s local=%t rate=%.2f\n", sample.Round, node, sample.Before.ClientServer, sample.Before.Leader, sample.Before.ClientIsLeader, sample.Measurement.AppendsSec)
		}
	}
	stream, err := all[0].Stream(ctx, "WF_JRN")
	if err != nil {
		return rep, err
	}
	info, err := stream.Info(ctx)
	if err != nil {
		return rep, err
	}
	rep.Messages, rep.Subjects = info.State.Msgs, info.State.NumSubjects
	if rep.Messages != uint64(entries*9) || rep.Subjects != 9 {
		return rep, fmt.Errorf("probe counts messages=%d subjects=%d", rep.Messages, rep.Subjects)
	}
	return rep, nil
}
