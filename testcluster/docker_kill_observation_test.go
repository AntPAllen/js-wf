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
			if receipt.KillStarted.After(receipt.KillReturned) || receipt.KillStarted.After(receipt.SourceStopped) || !receipt.ConcurrentObservation {
				t.Fatal(receipt)
			}
			if state == "running" || state == "removing" {
				if receipt.State != "absent" || !receipt.SourceStopped.Equal(receipt.CleanupComplete) {
					t.Fatal(receipt)
				}
			} else if receipt.State != state || !receipt.SourceStopped.Before(receipt.CleanupComplete) {
				t.Fatal(receipt)
			}
		})
	}
}

func TestDockerKillObservationPollWakesOnReplyWithoutTick(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	killed := make(chan dockerKillResult, 1)
	want := dockerKillResult{at: time.Date(2026, 10, 3, 0, 0, 0, 0, time.UTC)}
	killed <- want
	// No poll tick exists. Waiting for one loses an available reply and fails
	// by context cancellation, independently of wall-clock scheduling speed.
	result, err := awaitDockerKillPoll(ctx, killed, nil)
	if err != nil || result == nil || !result.at.Equal(want.at) || result.err != nil {
		t.Fatalf("kill reply did not wake state observer: result=%+v err=%v", result, err)
	}
}

func TestDockerKillObservationPollTicksAndCancellation(t *testing.T) {
	poll := make(chan time.Time, 1)
	poll <- time.Time{}
	result, err := awaitDockerKillPoll(context.Background(), nil, poll)
	if result != nil || err != nil {
		t.Fatalf("poll result=%+v err=%v", result, err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	result, err = awaitDockerKillPoll(ctx, nil, nil)
	if result != nil || !errors.Is(err, context.Canceled) {
		t.Fatalf("cancellation result=%+v err=%v", result, err)
	}
}

// The kill reply cannot arrive until a later state read. A sequential helper
// therefore cannot observe exit, even though the server has already stopped.
// Channel ordering proves the early bound without relying on sleep timing.
func TestDockerKillObservationDoesNotWaitForKillReply(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	started, reply := make(chan struct{}), make(chan struct{})
	reads := 0
	receipt, err := observeDockerKill(ctx, 4, "source", func(ctx context.Context, args ...string) (string, error) {
		if args[0] == "kill" {
			close(started)
			select {
			case <-reply:
				return "source", nil
			case <-ctx.Done():
				return "", ctx.Err()
			}
		}
		select {
		case <-started:
		case <-ctx.Done():
			return "", ctx.Err()
		}
		reads++
		if reads == 1 {
			return "exited", nil
		}
		close(reply)
		return "", nil
	})
	if err != nil {
		t.Fatalf("kill reply prevented observing stopped server: %v receipt=%+v", err, receipt)
	}
	if receipt.State != "exited" || !receipt.ConcurrentObservation || !receipt.SourceStopped.Before(receipt.KillReturned) || !receipt.SourceStopped.Before(receipt.CleanupComplete) {
		t.Fatalf("early exit bound was lost: %+v", receipt)
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
				if args[0] == "ps" && !autoRemove && observedExit {
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

func TestDockerKillObservationNativeDelayedReply(t *testing.T) {
	image := os.Getenv("WF_DOCKER_EXIT_IMAGE")
	if image == "" {
		t.Skip("set WF_DOCKER_EXIT_IMAGE to a local nats-server image")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	name := fmt.Sprintf("js-wf-exit-delayed-%d", time.Now().UnixNano())
	if _, err := dockerCommand(ctx, "run", "-d", "--name", name, image); err != nil {
		t.Fatal(err)
	}
	defer dockerCommand(context.Background(), "rm", "-f", name)
	reply := make(chan struct{})
	observedExit := false
	receipt, err := observeDockerKill(ctx, 4, name, func(ctx context.Context, args ...string) (string, error) {
		if args[0] == "kill" {
			out, err := dockerCommand(ctx, args...)
			if err != nil {
				return out, err
			}
			// Hold the actual successful command reply until a later state
			// observation cleans up this same stopped container.
			select {
			case <-reply:
				return out, nil
			case <-ctx.Done():
				return "", ctx.Err()
			}
		}
		if observedExit {
			if _, err := dockerCommand(ctx, "rm", name); err != nil {
				return "", err
			}
			close(reply)
		}
		out, err := dockerCommand(ctx, args...)
		if err == nil && out == "exited" {
			observedExit = true
		}
		return out, err
	})
	if err != nil {
		t.Fatal(err)
	}
	if receipt.State != "exited" || !receipt.ConcurrentObservation || !receipt.SourceStopped.Before(receipt.KillReturned) || !receipt.SourceStopped.Before(receipt.CleanupComplete) {
		t.Fatalf("actual stopped source bound waited for reply: %+v", receipt)
	}
	t.Logf("delayed actual Docker reply: %+v", receipt)
}
