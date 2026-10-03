//go:build linux

package testcluster

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/nats-io/nats.go"
)

type DockerGracefulUpgradeObservation struct {
	Node             int       `json:"node"`
	Container        string    `json:"container"`
	Signal           string    `json:"signal"`
	ServerID         string    `json:"server_id"`
	Version          string    `json:"version"`
	SignalStarted    time.Time `json:"signal_started"`
	SignalReturned   time.Time `json:"signal_returned"`
	LameDuckObserved time.Time `json:"lame_duck_observed"`
	SourceStopped    time.Time `json:"source_stopped"`
	CleanupComplete  time.Time `json:"cleanup_complete"`
	State            string    `json:"state"`
	ShutdownLogs     string    `json:"shutdown_logs"`
}

// UpgradeNodeGracefully signals the original process, proves its Lame Duck
// notification and clean shutdown, waits for name removal, then reuses its store.
// A timeout fails the profile; it never escalates to SIGKILL.
func (c *DockerCluster) UpgradeNodeGracefully(ctx context.Context, i int) (DockerGracefulUpgradeObservation, error) {
	proof := DockerGracefulUpgradeObservation{Node: i, Container: c.NodeName(i), Signal: "SIGUSR2"}
	if !c.usesOldBinary(i) {
		return proof, fmt.Errorf("Docker node%d is not an old-version node", i)
	}
	bound, stop := context.WithTimeout(ctx, 80*time.Second)
	defer stop()
	entered := make(chan time.Time, 1)
	nc, err := nats.Connect(c.ClientURL(i), nats.NoReconnect(), nats.IgnoreDiscoveredServers(), nats.Timeout(time.Second), nats.LameDuckModeHandler(func(*nats.Conn) {
		select {
		case entered <- time.Now().UTC():
		default:
		}
	}))
	if err != nil {
		return proof, err
	}
	defer nc.Close()
	proof.ServerID, proof.Version = nc.ConnectedServerId(), nc.ConnectedServerVersion()
	// Attach before signaling: --rm otherwise discards the old process log.
	logs := make(chan dockerGracefulLogs, 1)
	go func() {
		text, err := dockerCommand(bound, "logs", "--follow", proof.Container)
		logs <- dockerGracefulLogs{text, err}
	}()
	joined := false
	defer func() {
		stop()
		if !joined {
			<-logs
		}
	}()
	proof.SignalStarted = time.Now().UTC()
	if _, err = dockerCommand(bound, "kill", "--signal=SIGUSR2", proof.Container); err != nil {
		return proof, err
	}
	proof.SignalReturned = time.Now().UTC()
	for bound.Err() == nil {
		select {
		case at := <-entered:
			proof.LameDuckObserved = at
		default:
		}
		state, err := dockerCommand(bound, "ps", "-a", "--filter", "name=^/"+proof.Container+"$", "--format", "{{.State}}")
		if err != nil {
			return proof, err
		}
		at := time.Now().UTC()
		if proof.SourceStopped.IsZero() && (state == "" || state == "exited" || state == "dead") {
			proof.SourceStopped = at
			proof.State = state
			if state == "" {
				proof.State = "absent"
			}
		}
		if state == "" {
			proof.CleanupComplete = at
			var logErr error
			select {
			case result := <-logs:
				joined = true
				proof.ShutdownLogs = result.text
				logErr = result.err
			case <-bound.Done():
				return proof, bound.Err()
			}
			if proof.LameDuckObserved.IsZero() {
				select {
				case proof.LameDuckObserved = <-entered:
				default:
				}
			}
			if proof.LameDuckObserved.IsZero() {
				return proof, fmt.Errorf("graceful upgrade node%d: no Lame Duck notification", i)
			}
			if logErr != nil {
				return proof, logErr
			}
			for _, marker := range []string{"Entering lame duck mode", "Initiating Shutdown...", "Server Exiting.."} {
				if !strings.Contains(proof.ShutdownLogs, marker) {
					return proof, fmt.Errorf("graceful upgrade node%d: missing shutdown marker %q", i, marker)
				}
			}
			c.oldNodes[i] = false
			return proof, c.RestartNode(i)
		}
		select {
		case <-time.After(100 * time.Millisecond):
		case <-bound.Done():
			return proof, bound.Err()
		}
	}
	return proof, fmt.Errorf("graceful upgrade node%d: %w", i, bound.Err())
}

type dockerGracefulLogs struct {
	text string
	err  error
}
