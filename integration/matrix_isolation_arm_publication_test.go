//go:build linux

package integration_test

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func TestMatrixIsolationArmCannotExposeIncompleteToken(t *testing.T) {
	for _, previous := range []string{"", "previous-complete-token"} {
		t.Run(previous, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "worker-isolation-arm")
			if previous != "" {
				if err := os.WriteFile(path, []byte(previous), 0600); err != nil {
					t.Fatal(err)
				}
			}
			token := []byte("new-complete-acquisition-token")
			observed := false
			err := publishMatrixIsolationArmWithWriter(path, token, func(target string, data []byte, mode os.FileMode) error {
				file, err := os.OpenFile(target, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, mode)
				if err != nil {
					return err
				}
				defer file.Close()
				// Stop at the exact OS boundary that WriteFile can expose to a
				// worker: its target exists but none of the token is written.
				visible, readErr := os.ReadFile(path)
				observed = true
				if previous == "" {
					if !os.IsNotExist(readErr) {
						return errors.New("worker can observe incomplete acquisition token")
					}
				} else if readErr != nil || string(visible) != previous {
					return errors.New("replacement truncated the visible acquisition token")
				}
				_, err = file.Write(data)
				return err
			})
			if err != nil {
				t.Fatal(err)
			}
			visible, err := os.ReadFile(path)
			if !observed || err != nil || !bytes.Equal(visible, token) {
				t.Fatalf("published token=%q observed=%v err=%v", visible, observed, err)
			}
		})
	}
}

func TestMatrixIsolationFailedWritePreservesPublishedToken(t *testing.T) {
	path := filepath.Join(t.TempDir(), "worker-isolation-arm")
	previous := []byte("previous-complete-token")
	if err := os.WriteFile(path, previous, 0600); err != nil {
		t.Fatal(err)
	}
	failure := errors.New("injected write failure after partial token")
	err := publishMatrixIsolationArmWithWriter(path, []byte("next-complete-token"), func(target string, _ []byte, mode os.FileMode) error {
		if err := os.WriteFile(target, []byte("partial"), mode); err != nil {
			return err
		}
		return failure
	})
	visible, readErr := os.ReadFile(path)
	if !errors.Is(err, failure) || readErr != nil || !bytes.Equal(visible, previous) {
		t.Fatalf("failed write exposed token=%q read=%v write=%v", visible, readErr, err)
	}
}
