package main

import (
	"context"
	"encoding/json"
	"fmt"
	"time"
)

type physicalReplicaSnapshot struct {
	Node     int             `json:"node"`
	Observed time.Time       `json:"observed"`
	Data     json.RawMessage `json:"monitoring_response"`
}

type physicalDrainAudit struct {
	Version  int                       `json:"version"`
	At       time.Time                 `json:"at"`
	Replicas []physicalReplicaSnapshot `json:"replicas"`
	Error    string                    `json:"error,omitempty"`
}

func inspectPhysicalDrain(ctx context.Context, replicas int, snapshot func(context.Context, int) ([]byte, error)) physicalDrainAudit {
	audit := physicalDrainAudit{Version: 1, At: time.Now().UTC()}
	for node := 0; node < replicas; node++ {
		data, err := snapshot(ctx, node)
		if err != nil {
			audit.Error = fmt.Sprintf("replica %d monitoring: %v", node, err)
			return audit
		}
		if !json.Valid(data) {
			audit.Error = fmt.Sprintf("replica %d monitoring: invalid JSON", node)
			return audit
		}
		audit.Replicas = append(audit.Replicas, physicalReplicaSnapshot{Node: node, Observed: time.Now().UTC(), Data: append(json.RawMessage(nil), data...)})
	}
	return audit
}

// Parse every count from its retained response. Absent fields cannot stand for
// zero, and a repeated server cannot stand for another physical replica.
func physicalReplicaState(data []byte, partitions uint32) (server string, messages, pending, last uint64, err error) {
	var response struct {
		Server   string `json:"server_id"`
		Accounts []struct {
			Streams []struct {
				Name  string `json:"name"`
				State struct {
					Messages  *uint64 `json:"messages"`
					Last      *uint64 `json:"last_seq"`
					Consumers *uint32 `json:"consumer_count"`
				} `json:"state"`
				Consumers []struct {
					Name       string  `json:"name"`
					Pending    *uint64 `json:"num_pending"`
					AckPending *uint64 `json:"num_ack_pending"`
				} `json:"consumer_detail"`
			} `json:"stream_detail"`
		} `json:"account_details"`
	}
	if err = json.Unmarshal(data, &response); err != nil {
		return
	}
	server = response.Server
	if server == "" {
		err = fmt.Errorf("missing server identity")
		return
	}
	found := 0
	for _, account := range response.Accounts {
		for _, stream := range account.Streams {
			if stream.Name != "WF_RUN" {
				continue
			}
			found++
			if stream.State.Messages == nil || stream.State.Last == nil || stream.State.Consumers == nil || *stream.State.Consumers != partitions || len(stream.Consumers) != int(partitions) {
				err = fmt.Errorf("incomplete local WF_RUN state")
				return
			}
			messages, last = *stream.State.Messages, *stream.State.Last
			seen := map[string]bool{}
			for _, consumer := range stream.Consumers {
				if seen[consumer.Name] || consumer.Pending == nil || consumer.AckPending == nil {
					err = fmt.Errorf("incomplete local consumer state")
					return
				}
				seen[consumer.Name] = true
				if *consumer.Pending != 0 || *consumer.AckPending != 0 {
					pending = 1
				}
			}
			for p := uint32(0); p < partitions; p++ {
				if !seen[fmt.Sprintf("volume-%d", p)] {
					err = fmt.Errorf("missing volume consumer %d", p)
					return
				}
			}
		}
	}
	if found != 1 {
		err = fmt.Errorf("expected one local WF_RUN, got %d", found)
	}
	return
}

func (a physicalDrainAudit) complete(replicas int, partitions uint32) bool {
	if a.Version != 1 || a.Error != "" || a.At.IsZero() || len(a.Replicas) != replicas {
		return false
	}
	servers := map[string]bool{}
	var last uint64
	for node, snapshot := range a.Replicas {
		if snapshot.Node != node || snapshot.Observed.Before(a.At) {
			return false
		}
		server, messages, pending, sequence, err := physicalReplicaState(snapshot.Data, partitions)
		if err != nil || servers[server] || messages != 0 || pending != 0 || sequence == 0 {
			return false
		}
		if node != 0 && sequence != last {
			return false
		}
		servers[server], last = true, sequence
	}
	return true
}
