//go:build linux

package integrity

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime/pprof"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"js-wf/testcluster"
)

type profileCursorStream struct {
	jetstream.Stream
	replicas int
	creates  []int
}

func (s *profileCursorStream) CreateConsumer(ctx context.Context, cfg jetstream.ConsumerConfig) (jetstream.Consumer, error) {
	if cfg.Replicas != 5 {
		return nil, fmt.Errorf("expected R5 source read config")
	}
	cfg.Replicas = s.replicas
	consumer, err := s.Stream.CreateConsumer(ctx, cfg)
	if err != nil {
		return nil, err
	}
	actual, err := consumer.Info(ctx)
	if err != nil {
		return nil, err
	}
	if actual.Config.Replicas != s.replicas {
		return nil, fmt.Errorf("cursor replicas differ")
	}
	s.creates = append(s.creates, actual.Config.Replicas)
	return consumer, nil
}

// A diagnostic PASS means the comparisons ran and produced reports; each
// individual attempt retains its original20s deadline and its own verdict.
// WF_AUDIT_R5_PROFILE_STORES must be an independently verified disposable copy.
func TestStreamingAuditR5RetainedProfileDiagnostic(t *testing.T) {
	storesRoot := os.Getenv("WF_AUDIT_R5_PROFILE_STORES")
	if storesRoot == "" {
		t.Skip("opt-in R5 copied-store audit phase/cursor/window comparison")
	}
	if !filepath.IsAbs(storesRoot) {
		t.Fatal("copied store root must be absolute")
	}
	root := candidateNativeRoot(t)
	stores := map[int]string{}
	for node := 0; node < 5; node++ {
		stores[node] = filepath.Join(storesRoot, fmt.Sprintf("node-%d", node))
	}
	cluster, err := testcluster.StartDockerClusterWithRestoredIdentity(filepath.Join(root, "cluster"), 5, stores, os.Getenv("WF_AUDIT_R5_PROFILE_IDENTITY"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(cluster.Close)
	t.Cleanup(func() {
		for node := 0; node < 5; node++ {
			logs, err := cluster.Logs(node)
			if err != nil {
				t.Error(err)
				continue
			}
			if err := os.WriteFile(filepath.Join(root, fmt.Sprintf("server-%d.log", node)), []byte(logs), 0644); err != nil {
				t.Error(err)
			}
		}
	})
	var urls []string
	for node := 0; node < 5; node++ {
		urls = append(urls, cluster.ClientURL(node))
	}
	nc, err := nats.Connect(strings.Join(urls, ","), nats.MaxReconnects(-1), nats.ReconnectWait(20*time.Millisecond))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(nc.Close)
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatal(err)
	}
	ctx, stop := context.WithTimeout(context.Background(), 4*time.Minute)
	defer stop()
	count := nativeAuditProfileCount(t)
	want := Report{Invocations: count, Journals: count, Entries: count * 12, Terminal: count}
	lastReason := "no stream response"
	polls := 0
	for {
		ready := true
		for _, name := range []string{"WF_INV", "WF_JRN"} {
			call, done := context.WithTimeout(ctx, 2*time.Second)
			stream, err := js.Stream(call, name)
			done()
			if err != nil {
				lastReason = fmt.Sprintf("%s: %v", name, err)
				ready = false
				break
			}
			info := stream.CachedInfo()
			if info.Config.Replicas != 5 || info.Cluster == nil || info.Cluster.Leader == "" {
				lastReason = fmt.Sprintf("%s replicas=%d cluster=%+v", name, info.Config.Replicas, info.Cluster)
				ready = false
				break
			}
			expected := uint64(count)
			if name == "WF_JRN" {
				expected *= 12
			}
			if info.State.Msgs != expected {
				t.Fatalf("reopened source count=%d want=%d", info.State.Msgs, expected)
			}
		}
		if ready {
			break
		}
		if polls%20 == 0 {
			t.Logf("readiness server=%s reason=%s", nc.ConnectedServerName(), lastReason)
		}
		polls++
		if ctx.Err() != nil {
			t.Fatalf("copied-store readiness: %v; last=%s", ctx.Err(), lastReason)
		}
		time.Sleep(50 * time.Millisecond)
	}
	type phase struct {
		Stream         string
		Records        int
		Bytes          int
		ScanNS         int64
		VisitNS        int64
		CursorReplicas []int
	}
	type result struct {
		Label     string
		Window    uint64
		Replicas  int
		ElapsedNS int64
		Phases    []phase
		Report    Report
		Error     string
		Complete  bool
	}
	var results []result
	type variantConfig struct {
		label       string
		window      uint64
		replicas    int
		byteBounded bool
	}
	variants := []variantConfig{{"r5-512", 512, 5, false}, {"r1-512", 512, 1, false}, {"r5-4096", 4096, 5, false}, {"r5-512-recheck", 512, 5, false}}
	if os.Getenv("WF_AUDIT_R5_PROFILE_BYTE_BOUNDED") == "1" {
		variants = []variantConfig{{"r5-512", 512, 5, false}, {"r5-byte-4096", 4096, 5, true}, {"r5-byte-4096-recheck", 4096, 5, true}, {"r5-512-recheck", 512, 5, false}}
	}
	for _, variant := range variants {
		profile, err := os.Create(filepath.Join(root, variant.label+".pprof"))
		if err != nil {
			t.Fatal(err)
		}
		if err := pprof.StartCPUProfile(profile); err != nil {
			t.Fatal(err)
		}
		attempt, done := context.WithTimeout(ctx, 20*time.Second)
		started := time.Now()
		r := result{Label: variant.label, Window: variant.window, Replicas: variant.replicas}
		r.Report, err = checkUsingOptions(attempt, js, nil, func(call context.Context, stream jetstream.Stream, cutoff *uint64, visit func(*jetstream.RawStreamMsg) error) error {
			p := phase{Stream: stream.CachedInfo().Config.Name}
			observed := &profileCursorStream{Stream: stream, replicas: variant.replicas}
			began := time.Now()
			var scanErr error
			pprof.Do(call, pprof.Labels("audit_phase", p.Stream), func(call context.Context) {
				visitor := func(msg *jetstream.RawStreamMsg) error {
					p.Records++
					p.Bytes += len(msg.Data)
					entered := time.Now()
					err := visit(msg)
					p.VisitNS += time.Since(entered).Nanoseconds()
					return err
				}
				if variant.byteBounded {
					scanErr = scanByteBoundedThrough(call, observed, cutoff, visitor)
				} else {
					scanErr = scanBatchThroughWithSize(call, observed, cutoff, visitor, variant.window)
				}
			})
			p.ScanNS = time.Since(began).Nanoseconds()
			p.CursorReplicas = observed.creates
			r.Phases = append(r.Phases, p)
			return scanErr
		}, true, true)
		r.ElapsedNS = time.Since(started).Nanoseconds()
		done()
		pprof.StopCPUProfile()
		profile.Close()
		r.Error = fmt.Sprint(err)
		r.Complete = err == nil && r.Report == want
		results = append(results, r)
		data, _ := json.MarshalIndent(results, "", "  ")
		if writeErr := os.WriteFile(filepath.Join(root, "comparisons.json"), append(data, '\n'), 0644); writeErr != nil {
			t.Fatal(writeErr)
		}
		t.Logf("profile variant=%s complete=%v elapsed=%s phases=%+v report=%+v err=%v", r.Label, r.Complete, time.Duration(r.ElapsedNS), r.Phases, r.Report, err)
		if err != nil && !errors.Is(err, context.DeadlineExceeded) {
			t.Fatal(err)
		}
		if err == nil && !r.Complete {
			t.Fatalf("incorrect report=%+v want=%+v", r.Report, want)
		}
	}
}
