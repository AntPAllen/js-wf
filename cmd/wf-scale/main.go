// wf-scale measures the subject-index risk with one NATS process per replica.
// It is a standalone benchmark, not part of the correctness test suite.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"net"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"js-wf/provision"

	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

type sample struct {
	Subjects           int         `json:"subjects_per_stream"`
	Elapsed            string      `json:"publish_elapsed"`
	RSSBytes           [3]uint64   `json:"rss_bytes_per_node"`
	InvocationMessages uint64      `json:"invocation_messages"`
	JournalMessages    uint64      `json:"journal_messages"`
	InvocationSubjects uint64      `json:"invocation_subjects"`
	JournalSubjects    uint64      `json:"journal_subjects"`
	InfoMedian         [3][2]int64 `json:"stream_info_median_microseconds"`
	InfoMax            [3][2]int64 `json:"stream_info_max_microseconds"`
}

type report struct {
	ServerVersion string    `json:"server_version"`
	Workers       int       `json:"workers"`
	BaselineRSS   [3]uint64 `json:"baseline_rss_bytes_per_node"`
	Samples       []sample  `json:"samples"`
}

func main() {
	child := flag.Bool("server", false, "run one benchmark NATS server")
	countList := flag.String("counts", "100000,1000000", "ascending subject counts per stream")
	workers := flag.Int("workers", 96, "concurrent publishers")
	minAvailableMiB := flag.Int64("min-available-mib", 0, "stop if Linux MemAvailable falls below this many MiB; 0 disables the guard")
	root := flag.String("root", "", "store and report directory; default temporary")
	port := flag.Int("port", 0, "child client port")
	routePort := flag.Int("route-port", 0, "child route port")
	peerRoutePort := flag.Int("peer-route-port", 0, "child route peer port")
	node := flag.Int("node", 0, "child node index")
	flag.Parse()
	if *child {
		if err := runServer(*root, *node, *port, *routePort, *peerRoutePort); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		return
	}
	counts, err := parseCounts(*countList)
	if err != nil || *workers < 1 || *workers > 512 || *minAvailableMiB < 0 || *minAvailableMiB > 1<<40 {
		fmt.Fprintln(os.Stderr, "invalid counts, workers, or memory threshold:", err)
		os.Exit(2)
	}
	if err := run(counts, *workers, *root, *minAvailableMiB); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func parseCounts(raw string) ([]int, error) {
	var counts []int
	prev := 0
	for _, part := range strings.Split(raw, ",") {
		n, err := strconv.Atoi(strings.TrimSpace(part))
		if err != nil || n <= prev || n > 10000000 {
			return nil, fmt.Errorf("counts must increase from 1 to 10000000: %q", raw)
		}
		counts = append(counts, n)
		prev = n
	}
	return counts, nil
}

func runServer(root string, node, port, routePort, peerPort int) error {
	if root == "" || node < 0 || node > 2 || port < 1 || routePort < 1 || peerPort < 1 {
		return errors.New("invalid server arguments")
	}
	opts := &server.Options{Host: "127.0.0.1", Port: port, JetStream: true, StoreDir: filepath.Join(root, fmt.Sprintf("node-%d", node)), NoLog: true, NoSigs: true, ServerName: fmt.Sprintf("wf-scale-%d", node)}
	opts.Accounts = []*server.Account{server.NewAccount("$SYS"), server.NewAccount("$G")}
	opts.SystemAccount = "$SYS"
	opts.Cluster = server.ClusterOpts{Name: "wf-scale", Host: "127.0.0.1", Port: routePort}
	u, _ := url.Parse(fmt.Sprintf("nats://127.0.0.1:%d", peerPort))
	opts.Routes = []*url.URL{u}
	s, err := server.NewServer(opts)
	if err != nil {
		return err
	}
	s.Start()
	s.WaitForShutdown()
	return nil
}

func freePort() (int, error) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return 0, err
	}
	defer l.Close()
	return l.Addr().(*net.TCPAddr).Port, nil
}

func run(counts []int, workers int, root string, minAvailableMiB int64) (runErr error) {
	if root == "" {
		var err error
		root, err = os.MkdirTemp("", "wf-scale-")
		if err != nil {
			return err
		}
		defer func() {
			if runErr == nil {
				_ = os.RemoveAll(root)
			} else {
				fmt.Fprintln(os.Stderr, "benchmark files:", root)
			}
		}()
	} else if err := os.MkdirAll(root, 0755); err != nil {
		return err
	}
	if _, err := os.Stat(filepath.Join(root, "node-0")); err == nil {
		return fmt.Errorf("benchmark root %s already contains a node store; use a fresh directory", root)
	} else if !os.IsNotExist(err) {
		return err
	}
	exe, err := os.Executable()
	if err != nil {
		return err
	}
	var ports, routes [3]int
	used := map[int]bool{}
	for i := 0; i < 3; i++ {
		for _, slot := range []*int{&ports[i], &routes[i]} {
			for {
				p, err := freePort()
				if err != nil {
					return err
				}
				if !used[p] {
					*slot = p
					used[p] = true
					break
				}
			}
		}
	}
	var children [3]*exec.Cmd
	var logs [3]*os.File
	defer func() {
		for _, child := range children {
			if child != nil && child.Process != nil {
				_ = child.Process.Kill()
			}
		}
		for _, child := range children {
			if child != nil && child.Process != nil {
				_ = child.Wait()
			}
		}
		for _, log := range logs {
			if log != nil {
				_ = log.Close()
			}
		}
	}()
	for i := 0; i < 3; i++ {
		log, err := os.Create(filepath.Join(root, fmt.Sprintf("node-%d.log", i)))
		if err != nil {
			return err
		}
		logs[i] = log
		peer := 0
		if i == 0 {
			peer = 1
		}
		child := exec.Command(exe, "-server", "-root", root, "-node", strconv.Itoa(i), "-port", strconv.Itoa(ports[i]), "-route-port", strconv.Itoa(routes[i]), "-peer-route-port", strconv.Itoa(routes[peer]))
		child.Stdout, child.Stderr = log, log
		if err := child.Start(); err != nil {
			return err
		}
		children[i] = child
	}
	var conns [3]*nats.Conn
	var all [3]jetstream.JetStream
	defer func() {
		for _, nc := range conns {
			if nc != nil {
				nc.Close()
			}
		}
	}()
	readyUntil := time.Now().Add(60 * time.Second)
	for i := 0; i < 3; i++ {
		for time.Now().Before(readyUntil) {
			nc, err := nats.Connect(fmt.Sprintf("nats://127.0.0.1:%d", ports[i]), nats.NoReconnect())
			if err == nil {
				conns[i] = nc
				all[i], err = jetstream.New(nc)
				if err != nil {
					return err
				}
				break
			}
			time.Sleep(100 * time.Millisecond)
		}
		if conns[i] == nil {
			return fmt.Errorf("node %d did not start; see %s", i, root)
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Minute)
	defer cancel()
	guardCtx, stopGuard := context.WithCancelCause(ctx)
	defer stopGuard(nil)
	if minAvailableMiB > 0 {
		go watchAvailableMemory(guardCtx, uint64(minAvailableMiB)*1024*1024, stopGuard)
	}
	for {
		attempt, stop := context.WithTimeout(ctx, 5*time.Second)
		err := provision.Ensure(attempt, all[0], 3)
		stop()
		if err == nil {
			break
		}
		if time.Now().After(readyUntil) {
			return fmt.Errorf("provision: %w", err)
		}
		time.Sleep(100 * time.Millisecond)
	}
	report := report{ServerVersion: conns[0].ConnectedServerVersion(), Workers: workers}
	if err := readRSS(children, &report.BaselineRSS); err != nil {
		return err
	}
	previous := 0
	for _, count := range counts {
		started := time.Now()
		publishErr := publishRange(guardCtx, all, previous, count, workers)
		if err := context.Cause(guardCtx); err != nil {
			return err
		}
		if publishErr != nil {
			return publishErr
		}
		time.Sleep(3 * time.Second)
		if err := context.Cause(guardCtx); err != nil {
			return err
		}
		s := sample{Subjects: count, Elapsed: time.Since(started).String()}
		if err := readRSS(children, &s.RSSBytes); err != nil {
			return err
		}
		if err := inspect(guardCtx, all, &s); err != nil {
			if cause := context.Cause(guardCtx); cause != nil {
				return cause
			}
			return err
		}
		report.Samples = append(report.Samples, s)
		data, err := json.MarshalIndent(report, "", "  ")
		if err != nil {
			return err
		}
		data = append(data, '\n')
		if err := os.WriteFile(filepath.Join(root, "report.json"), data, 0644); err != nil {
			return err
		}
		fmt.Fprintf(os.Stderr, "subjects=%d rss=%v info_median_us=%v\n", count, s.RSSBytes, s.InfoMedian)
		previous = count
	}
	return nil
}

func publishRange(ctx context.Context, all [3]jetstream.JetStream, first, last, workers int) error {
	jobs := make(chan int, 512)
	var wg sync.WaitGroup
	var firstErr error
	var once sync.Once
	batch, cancel := context.WithCancel(ctx)
	defer cancel()
	for worker := 0; worker < workers; worker++ {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			js := all[worker%3]
			for id := range jobs {
				for _, prefix := range []string{"wf.inv.scale.", "wf.jrn.scale."} {
					attempt, stop := context.WithTimeout(batch, 10*time.Second)
					_, err := js.Publish(attempt, prefix+strconv.Itoa(id), []byte("x"))
					stop()
					if err != nil {
						once.Do(func() { firstErr = fmt.Errorf("publish %s%d: %w", prefix, id, err); cancel() })
						return
					}
				}
			}
		}(worker)
	}
producer:
	for id := first; id < last; id++ {
		select {
		case jobs <- id:
		case <-batch.Done():
			break producer
		}
	}
	close(jobs)
	wg.Wait()
	return firstErr
}

func readRSS(children [3]*exec.Cmd, out *[3]uint64) error {
	for i, child := range children {
		data, err := os.ReadFile(fmt.Sprintf("/proc/%d/status", child.Process.Pid))
		if err != nil {
			return err
		}
		for _, line := range strings.Split(string(data), "\n") {
			if strings.HasPrefix(line, "VmRSS:") {
				fields := strings.Fields(line)
				kb, err := strconv.ParseUint(fields[1], 10, 64)
				if err != nil {
					return err
				}
				out[i] = kb * 1024
				break
			}
		}
		if out[i] == 0 {
			return fmt.Errorf("no VmRSS for node %d", i)
		}
	}
	return nil
}

func watchAvailableMemory(ctx context.Context, minimum uint64, stop context.CancelCauseFunc) {
	ticker := time.NewTicker(250 * time.Millisecond)
	defer ticker.Stop()
	for {
		available, err := availableMemory()
		if err != nil {
			stop(fmt.Errorf("memory guard: %w", err))
			return
		}
		if available < minimum {
			stop(fmt.Errorf("memory guard: MemAvailable %d MiB below %d MiB", available/(1024*1024), minimum/(1024*1024)))
			return
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func availableMemory() (uint64, error) {
	data, err := os.ReadFile("/proc/meminfo")
	if err != nil {
		return 0, err
	}
	for _, line := range strings.Split(string(data), "\n") {
		if !strings.HasPrefix(line, "MemAvailable:") {
			continue
		}
		fields := strings.Fields(line)
		if len(fields) != 3 || fields[2] != "kB" {
			return 0, fmt.Errorf("invalid MemAvailable line %q", line)
		}
		kb, err := strconv.ParseUint(fields[1], 10, 64)
		if err != nil {
			return 0, err
		}
		return kb * 1024, nil
	}
	return 0, errors.New("MemAvailable missing from /proc/meminfo")
}

func inspect(ctx context.Context, all [3]jetstream.JetStream, s *sample) error {
	for node, js := range all {
		for streamIndex, name := range []string{"WF_INV", "WF_JRN"} {
			stream, err := js.Stream(ctx, name)
			if err != nil {
				return err
			}
			var times []int64
			for trial := 0; trial < 5; trial++ {
				attempt, stop := context.WithTimeout(ctx, 30*time.Second)
				start := time.Now()
				info, err := stream.Info(attempt)
				times = append(times, time.Since(start).Microseconds())
				stop()
				if err != nil {
					return err
				}
				if info.State.Msgs != uint64(s.Subjects) || info.State.NumSubjects != uint64(s.Subjects) {
					return fmt.Errorf("node %d stream %s: msgs=%d subjects=%d want=%d", node, name, info.State.Msgs, info.State.NumSubjects, s.Subjects)
				}
				if node == 0 {
					if streamIndex == 0 {
						s.InvocationMessages, s.InvocationSubjects = info.State.Msgs, info.State.NumSubjects
					} else {
						s.JournalMessages, s.JournalSubjects = info.State.Msgs, info.State.NumSubjects
					}
				}
			}
			sort.Slice(times, func(i, j int) bool { return times[i] < times[j] })
			s.InfoMedian[node][streamIndex] = times[len(times)/2]
			s.InfoMax[node][streamIndex] = times[len(times)-1]
		}
	}
	return nil
}
