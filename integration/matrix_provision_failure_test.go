//go:build linux

package integration_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"js-wf/testcluster"
)

type matrixDiagnosticSource interface {
	Diagnostic(context.Context, int, string) ([]byte, error)
	NodeName(int) string
}

type matrixProvisionObservation struct {
	Node     int       `json:"node"`
	Name     string    `json:"name"`
	Kind     string    `json:"kind"`
	Started  time.Time `json:"started"`
	Finished time.Time `json:"finished"`
	File     string    `json:"file,omitempty"`
	Error    string    `json:"error,omitempty"`
}

// The provisioning verdict is already failed. Observe all nodes concurrently
// within a separate bounded diagnostic context; never retry or change admission.
// Diagnostic reads use pinned host monitoring ports, not the failed NATS client.
func saveMatrixProvisionFailure(ctx context.Context, root string, source matrixDiagnosticSource, cause error) error {
	if err := saveMatrixFailureStack(root, true); err != nil {
		return err
	}
	observations := make([]matrixProvisionObservation, 10)
	var group sync.WaitGroup
	for node := 0; node < 5; node++ {
		for kindIndex, kind := range []string{"jetstream", "routes"} {
			index := node*2 + kindIndex
			group.Add(1)
			go func() {
				defer group.Done()
				o := matrixProvisionObservation{Node: node, Name: source.NodeName(node), Kind: kind, Started: time.Now().UTC()}
				data, err := source.Diagnostic(ctx, node, kind)
				o.Finished = time.Now().UTC()
				if err != nil {
					o.Error = err.Error()
				}
				// Keep partial/malformed responses too: observations are raw evidence,
				// never parsed into a healthy quorum or a server-cause conclusion.
				if len(data) > 0 {
					o.File = fmt.Sprintf("provision-failure-node-%d-%s.raw", node, kind)
					if writeErr := os.WriteFile(filepath.Join(root, o.File), data, 0600); writeErr != nil {
						o.Error = errors.Join(err, writeErr).Error()
						o.File = ""
					}
				}
				observations[index] = o
			}()
		}
	}
	group.Wait()
	report := struct {
		Cause        string                       `json:"provisioning_error"`
		Scope        string                       `json:"scope"`
		Observations []matrixProvisionObservation `json:"observations"`
	}{cause.Error(), "Post-verdict monitoring observations before cleanup; not simultaneous state, placement eligibility proof, server-cause attribution or successful workload evidence", observations}
	data, err := json.MarshalIndent(report, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(filepath.Join(root, "provision-failure.json"), data, 0600)
}

type blockedMatrixDiagnostics struct {
	entered  chan struct{}
	response []byte
	err      error
}

func (d blockedMatrixDiagnostics) NodeName(node int) string { return fmt.Sprintf("node-%d", node) }
func (d blockedMatrixDiagnostics) Diagnostic(ctx context.Context, node int, kind string) ([]byte, error) {
	d.entered <- struct{}{}
	<-ctx.Done()
	if d.response != nil {
		return d.response, d.err
	}
	return nil, ctx.Err()
}

func TestMatrixProvisionFailureCapturesConcurrentCancelledReads(t *testing.T) {
	for _, partial := range []bool{false, true} {
		t.Run(fmt.Sprint(partial), func(t *testing.T) {
			root := t.TempDir()
			ctx, stop := context.WithCancel(context.Background())
			defer stop()
			d := blockedMatrixDiagnostics{entered: make(chan struct{}, 10)}
			if partial {
				d.response = []byte("partial malformed monitoring bytes\n")
				d.err = errors.New("truncated response")
			}
			done := make(chan error, 1)
			go func() { done <- saveMatrixProvisionFailure(ctx, root, d, errors.New("placement rejected")) }()
			// All ten reads must enter before any can finish; a sequential capture
			// cannot pass, and cancellation must release every blocked request.
			deadline := time.After(5 * time.Second)
			for i := 0; i < 10; i++ {
				select {
				case <-d.entered:
				case <-deadline:
					t.Fatal("monitoring reads did not enter concurrently")
				}
			}
			stop()
			select {
			case err := <-done:
				if err != nil {
					t.Fatal(err)
				}
			case <-deadline:
				t.Fatal("cancelled monitoring reads did not finish")
			}
			data, err := os.ReadFile(filepath.Join(root, "provision-failure.json"))
			if err != nil {
				t.Fatal(err)
			}
			var report struct {
				Cause        string                       `json:"provisioning_error"`
				Observations []matrixProvisionObservation `json:"observations"`
			}
			if err := json.Unmarshal(data, &report); err != nil {
				t.Fatal(err)
			}
			if report.Cause != "placement rejected" || len(report.Observations) != 10 {
				t.Fatalf("lost original failure: %+v", report)
			}
			for index, o := range report.Observations {
				if o.Node != index/2 || o.Name != fmt.Sprintf("node-%d", index/2) || o.Error == "" || o.Started.IsZero() || o.Finished.Before(o.Started) {
					t.Fatalf("incomplete observation: %+v", o)
				}
				if partial {
					raw, err := os.ReadFile(filepath.Join(root, o.File))
					if err != nil || string(raw) != string(d.response) {
						t.Fatalf("partial response lost: %q %v", raw, err)
					}
				} else if o.File != "" {
					t.Fatal("missing response invented a raw file")
				}
			}
			if _, err := os.Stat(filepath.Join(root, "failure-goroutines.txt")); err != nil {
				t.Fatal(err)
			}
		})
	}
}

// Validate the actual pinned monitoring transport with one paused Docker node.
// The failure cause is injected: this does not reproduce seed75 placement.
func TestMatrixProvisionFailureNativePausedMonitor(t *testing.T) {
	root := os.Getenv("WF_PROVISION_FAILURE_NATIVE_ROOT")
	if root == "" {
		t.Skip("set WF_PROVISION_FAILURE_NATIVE_ROOT to a fresh absolute artifact directory")
	}
	if !filepath.IsAbs(root) {
		t.Fatal("artifact directory must be absolute")
	}
	if err := os.Mkdir(root, 0755); err != nil {
		t.Fatal(err)
	}
	cluster, err := testcluster.StartDockerCluster(filepath.Join(root, "cluster"), 5)
	if err != nil {
		t.Fatal(err)
	}
	defer cluster.Close()
	defer func() {
		for node := 0; node < 5; node++ {
			logs, err := cluster.Logs(node)
			if err != nil {
				t.Error(err)
				continue
			}
			if err := os.WriteFile(filepath.Join(root, fmt.Sprintf("server-%d.log", node)), []byte(logs), 0600); err != nil {
				t.Error(err)
			}
		}
	}()
	if err := cluster.PauseNode(4); err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := cluster.UnpauseNode(4); err != nil {
			t.Error(err)
		}
	}()
	ctx, stop := context.WithTimeout(context.Background(), 2*time.Second)
	defer stop()
	if err := saveMatrixProvisionFailure(ctx, root, cluster, errors.New("injected diagnostic control; not a placement failure reproduction")); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(filepath.Join(root, "provision-failure.json"))
	if err != nil {
		t.Fatal(err)
	}
	var report struct {
		Observations []matrixProvisionObservation `json:"observations"`
	}
	if err := json.Unmarshal(data, &report); err != nil {
		t.Fatal(err)
	}
	if len(report.Observations) != 10 {
		t.Fatal("lost monitoring outcomes")
	}
	identities := map[string]bool{}
	for _, o := range report.Observations {
		if o.Name != cluster.NodeName(o.Node) {
			t.Fatalf("wrong pinned node: %+v", o)
		}
		if o.Node == 4 {
			if o.Error == "" || o.File != "" {
				t.Fatalf("paused monitor claimed a response: %+v", o)
			}
			continue
		}
		if o.Error != "" || o.File == "" {
			t.Fatalf("responsive monitor lost: %+v", o)
		}
		raw, err := os.ReadFile(filepath.Join(root, o.File))
		if err != nil {
			t.Fatal(err)
		}
		var response struct {
			ID   string `json:"server_id"`
			Name string `json:"server_name"`
		}
		if err := json.Unmarshal(raw, &response); err != nil || response.ID == "" {
			t.Fatalf("invalid native response: %s err=%v", raw, err)
		}
		if o.Kind == "routes" {
			if response.Name != o.Name || identities[response.ID] {
				t.Fatal("wrong/duplicate native monitor identity")
			}
			identities[response.ID] = true
		}
	}
	if len(identities) != 4 {
		t.Fatal("lost responsive native nodes")
	}
}
