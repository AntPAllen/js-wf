// Package graphcli keeps experimental graph store selection consistent between
// the operator and worker commands. Selection never imports or provisions data.
package graphcli

import (
	"fmt"
	"js-wf/journal"
)

// Select requires every participant to explicitly choose the same cursor schema.
// Version 4 is the existing default; version 6 requires new isolated stores.
func Select(authority, prefix, bucket string, replicas int, encoding journal.Encoding, version int) (*journal.NativeGraphConfig, error) {
	if version < 4 || version > 6 {
		return nil, fmt.Errorf("graph-cursor-version must be 4, 5 or 6")
	}
	if authority == "" && prefix == "" && bucket == "" {
		if version != 4 {
			return nil, fmt.Errorf("graph-cursor-version %d requires graph-authority-stream, graph-authority-prefix and graph-object-bucket together", version)
		}
		return nil, nil
	}
	if authority == "" || prefix == "" || bucket == "" {
		return nil, fmt.Errorf("graph runtime requires graph-authority-stream, graph-authority-prefix and graph-object-bucket together")
	}
	cfg := journal.NativeGraphConfig{AuthorityStream: authority, AuthorityPrefix: prefix, ObjectBucket: bucket, ExpectedReplicas: replicas, Encoding: encoding, CanonicalStarts: true, CanonicalSignals: true, CheckpointIndex: version >= 5, ArchiveCheckpoints: version == 6}
	if _, err := journal.NativeGraphStreamConfigs(cfg, replicas); err != nil {
		return nil, err
	}
	return &cfg, nil
}
