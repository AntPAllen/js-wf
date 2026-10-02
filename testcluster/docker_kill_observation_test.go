//go:build linux

package testcluster

import (
	"context"
	"errors"
	"fmt"
	"os"
	"testing"
	"time"
)

func TestDockerKillObservationSeparatesExitFromCleanup(t *testing.T) {
	for _, state := range []string{"exited", "dead", "running", "removing"} {
		t.Run(state, func(t *testing.T) {
			calls := 0
			receipt, err := observeDockerKill(context.Background(), 4, "source", func(_ context.Context, args ...string) (string, error) {
				if args[0] == "kill" {
					return "source", nil
				}
				calls++
				if calls == 1 {
					return state, nil
				}
				return "", nil
			})
			if err != nil {
				t.Fatal(err)
			}
			if receipt.KillStarted.After(receipt.KillReturned) || receipt.KillReturned.After(receipt.SourceStopped) {
				t.Fatal(receipt)
			}
			if state == "running" || state == "removing" {
				if receipt.State != "absent" || !receipt.SourceStopped.Equal(receipt.CleanupComplete) {
					t.Fatal(receipt)
				}
			} else if receipt.State != state || receipt.CleanupComplete.Sub(receipt.SourceStopped) < 100*time.Millisecond {
				t.Fatal(receipt)
			}
		})
	}
}

func TestDockerKillObservationRejectsFailedListing(t *testing.T) {
	receipt, err := observeDockerKill(context.Background(), 4, "source", func(_ context.Context, args ...string) (string, error) {
		if args[0] == "kill" {
			return "source", nil
		}
		return "", errors.New("daemon unavailable")
	})
	if err == nil || !receipt.SourceStopped.IsZero() {
		t.Fatalf("receipt=%+v err=%v", receipt, err)
	}
}

func TestDockerKillObservationNative(t *testing.T) {
	image := os.Getenv("WF_DOCKER_EXIT_IMAGE")
	if image == "" {
		t.Skip("set WF_DOCKER_EXIT_IMAGE to a local nats-server image")
	}
	for _, autoRemove := range []bool{false, true} {
		t.Run(fmt.Sprintf("auto-remove-%t", autoRemove), func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			name := fmt.Sprintf("js-wf-exit-proof-%d", time.Now().UnixNano())
			args := []string{"run", "-d", "--name", name}
			if autoRemove {
				args = append(args, "--rm")
			}
			args = append(args, image)
			if _, err := dockerCommand(ctx, args...); err != nil {
				t.Fatal(err)
			}
			defer dockerCommand(context.Background(), "rm", "-f", name)
			observedExit := false
			receipt, err := observeDockerKill(ctx, 4, name, func(ctx context.Context, args ...string) (string, error) {
				if !autoRemove && observedExit && args[0] == "ps" {
					if _, err := dockerCommand(ctx, "rm", name); err != nil {
						return "", err
					}
				}
				out, err := dockerCommand(ctx, args...)
				if err == nil && args[0] == "ps" && out == "exited" {
					observedExit = true
				}
				return out, err
			})
			if err != nil {
				t.Fatal(err)
			}
			if receipt.SourceStopped.IsZero() || receipt.SourceStopped.After(receipt.CleanupComplete) {
				t.Fatal(receipt)
			}
			if !autoRemove && (receipt.State != "exited" || !receipt.SourceStopped.Before(receipt.CleanupComplete)) {
				t.Fatal(receipt)
			}
			t.Logf("confirmed exit receipt: %+v", receipt)
		})
	}
}
