//go:build linux

package integration_test

import (
	"context"
	"fmt"
	"time"

	"js-wf/testcluster"
)

// Clock observations come from the actual server monitoring endpoint, bracketed
// by the unshifted controller's clock. Configuration alone is not skew proof.
type matrixServerClockObservation struct {
	Stage            string    `json:"stage"`
	Fault            int       `json:"fault"`
	Node             int       `json:"node"`
	HostBefore       time.Time `json:"host_before"`
	ServerNow        time.Time `json:"server_now"`
	HostAfter        time.Time `json:"host_after"`
	ExpectedOffsetNS int64     `json:"expected_offset_ns"`
}

func matrixServerClockOffset(row string) time.Duration {
	switch row {
	case "server_clock_ahead":
		return time.Minute
	case "server_clock_behind":
		return -time.Minute
	default:
		return 0
	}
}

func observeMatrixServerClocks(ctx context.Context, cluster *testcluster.DockerCluster, row, stage string, fault int) ([]matrixServerClockObservation, error) {
	offset := matrixServerClockOffset(row)
	if offset == 0 {
		return nil, fmt.Errorf("not a server clock row: %s", row)
	}
	var records []matrixServerClockObservation
	for node := 0; node < 5; node++ {
		want := time.Duration(0)
		if node == 4 {
			want = offset
		}
		attempt, stop := context.WithTimeout(ctx, 2*time.Second)
		before := time.Now().UTC()
		serverNow, err := cluster.ServerNow(attempt, node)
		after := time.Now().UTC()
		stop()
		if err != nil {
			return records, fmt.Errorf("%s fault%d node%d clock: %w", stage, fault, node, err)
		}
		records = append(records, matrixServerClockObservation{stage, fault, node, before, serverNow, after, int64(want)})
		if after.Sub(before) > 2*time.Second || serverNow.Before(before.Add(want-2*time.Second)) || serverNow.After(after.Add(want+2*time.Second)) {
			return records, fmt.Errorf("%s fault%d node%d server clock=%s host interval=%s..%s expected offset=%s", stage, fault, node, serverNow, before, after, want)
		}
	}
	return records, nil
}
