// wf-timer-volume verifies native scheduled messages at volume across two full
// process-cluster restarts. It uses production timer publication, not workflows.
package main

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"runtime/debug"
	"sort"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"js-wf/identity"
	"js-wf/provision"
	"js-wf/testcluster"
	"js-wf/worker"
)

type config struct {
	Count, Publishers                int
	Horizon, Lead, P99Limit, MaxLate time.Duration
	Root                             string
}
type killedProcess struct {
	PID    int
	Signal string
}

type restart struct {
	Killed          []killedProcess
	Started, Healed time.Time
	ReceivedBefore  int
}
type report struct {
	Revision                       string `json:"revision,omitempty"`
	SourceModified                 string `json:"source_modified,omitempty"`
	Storage                        string `json:"storage"`
	Replicas                       int    `json:"replicas"`
	Partitions                     uint32 `json:"partitions"`
	Publishers                     int    `json:"publishers"`
	Lead                           string `json:"lead"`
	ObservationsSHA256             string `json:"observations_sha256,omitempty"`
	Status                         string `json:"status"`
	Error                          string `json:"error,omitempty"`
	GoVersion                      string `json:"go_version"`
	ServerVersion                  string `json:"server_version"`
	Count                          int    `json:"scheduled_count"`
	Published                      int    `json:"acknowledged_publishes"`
	Received                       int    `json:"unique_received"`
	Redeliveries                   int    `json:"redeliveries"`
	AckErrors                      int    `json:"ack_errors"`
	FetchErrors                    int    `json:"fetch_errors"`
	LastFetchError                 string `json:"last_fetch_error,omitempty"`
	Horizon                        string `json:"horizon"`
	FirstDue, LastDue              time.Time
	P99Limit, MaxLateLimit         string
	P99LateSeconds, MaxLateSeconds float64
	Restarts                       []restart
	FinalMessages                  uint64 `json:"final_stream_messages"`
	FinalAckPending                int    `json:"final_ack_pending"`
	Updated                        time.Time
}
type observation struct {
	Sequence uint64
	At       time.Time
}
type campaign struct {
	mu   sync.Mutex
	cfg  config
	rep  report
	seen []observation
	base time.Time
}

func main() {
	var c config
	flag.IntVar(&c.Count, "count", 1000000, "number of distinct native schedules")
	flag.IntVar(&c.Publishers, "publishers", 64, "concurrent schedule publishers")
	flag.DurationVar(&c.Horizon, "horizon", 24*time.Hour, "first-to-last scheduled deadline span")
	flag.DurationVar(&c.Lead, "lead", 15*time.Minute, "load runway before the first deadline")
	flag.DurationVar(&c.P99Limit, "p99-limit", 2*time.Second, "maximum raw delivery lateness p99")
	flag.DurationVar(&c.MaxLate, "max-late", 30*time.Second, "maximum individual raw delivery lateness")
	flag.StringVar(&c.Root, "root", "", "new directory for stores, logs and progress/results; required")
	verify := flag.Bool("verify", false, "verify a completed campaign report and every observation offline")
	allowSmoke := flag.Bool("allow-smoke", false, "allow verification below the million-message/24-hour release workload")
	flag.Parse()
	if *verify {
		if err := verifyReport(c.Root, *allowSmoke); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		fmt.Println("campaign observations and gates verified")
		return
	}
	if err := validate(c); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
	if err := run(c); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
func validate(c config) error {
	if c.Count < 3 || c.Count > 10000000 || c.Publishers < 1 || c.Publishers > 512 || c.Horizon < 6*time.Second || c.Horizon > 7*24*time.Hour || c.Lead < 5*time.Second || c.Lead > time.Hour || c.P99Limit <= 0 || c.MaxLate < c.P99Limit || c.Root == "" {
		return fmt.Errorf("invalid campaign configuration")
	}
	return nil
}
func due(base time.Time, horizon time.Duration, count, index int) time.Time {
	// Split the duration to avoid multiplying a 24h nanosecond span by a million.
	denominator := int64(count - 1)
	return base.Add(time.Duration(int64(horizon)/denominator*int64(index) + int64(horizon)%denominator*int64(index)/denominator))
}
func (c *campaign) save() error {
	c.mu.Lock()
	rep := c.rep
	rep.Restarts = append([]restart(nil), c.rep.Restarts...)
	late := make([]float64, 0, c.rep.Received)
	for i, seen := range c.seen {
		if seen.Sequence != 0 {
			late = append(late, seen.At.Sub(due(c.base, c.cfg.Horizon, c.cfg.Count, i)).Seconds())
		}
	}
	c.mu.Unlock()
	sort.Float64s(late)
	if len(late) > 0 {
		rep.P99LateSeconds = late[(99*len(late)+99)/100-1]
		rep.MaxLateSeconds = late[len(late)-1]
	}
	rep.Updated = time.Now().UTC()
	data, err := json.MarshalIndent(rep, "", "  ")
	if err != nil {
		return err
	}
	// Atomic checkpoints survive interruption without claiming the run passed.
	path := filepath.Join(c.cfg.Root, "report.json")
	if err = os.WriteFile(path+".tmp", append(data, '\n'), 0644); err != nil {
		return err
	}
	return os.Rename(path+".tmp", path)
}
func (c *campaign) observe(msg jetstream.Msg, partition uint32) error {
	key := strings.TrimPrefix(string(msg.Data()), "volume.")
	index, err := strconv.Atoi(key)
	if err != nil || index < 0 || index >= c.cfg.Count || string(msg.Data()) != identity.Key("volume", strconv.Itoa(index)) {
		return fmt.Errorf("invalid delivered identity %q", msg.Data())
	}
	if msg.Subject() != identity.RunSubject("volume", key, provision.Partitions) || identity.Partition("volume", key, provision.Partitions) != partition {
		return fmt.Errorf("wrong target for timer %d: %s", index, msg.Subject())
	}
	if msg.Headers().Get(identity.TimerInvSeqHeader) != strconv.Itoa(index+1) || msg.Headers().Get(identity.TimerStepHeader) != "0" {
		return fmt.Errorf("timer %d generation/step changed", index)
	}
	meta, err := msg.Metadata()
	if err != nil {
		return err
	}
	deadline := due(c.base, c.cfg.Horizon, c.cfg.Count, index)
	now := time.Now()
	if meta.Timestamp.Before(deadline) || now.Before(deadline) {
		return fmt.Errorf("timer %d fired early: server=%s due=%s received=%s", index, meta.Timestamp, deadline, now)
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	seen := &c.seen[index]
	if seen.Sequence != 0 {
		if seen.Sequence != meta.Sequence.Stream {
			return fmt.Errorf("timer %d emitted twice at stream sequences %d/%d", index, seen.Sequence, meta.Sequence.Stream)
		}
		c.rep.Redeliveries++
	} else {
		*seen = observation{Sequence: meta.Sequence.Stream, At: now}
		c.rep.Received++
	}
	return nil
}
func waitReady(ctx context.Context, js jetstream.JetStream) (jetstream.Stream, error) {
	var last error
	for ctx.Err() == nil {
		attempt, done := context.WithTimeout(ctx, 3*time.Second)
		stream, err := js.Stream(attempt, "WF_RUN")
		if err == nil {
			info, infoErr := stream.Info(attempt)
			err = infoErr
			if err == nil && info.Cluster != nil && info.Cluster.Leader != "" && len(info.Cluster.Replicas) == 2 {
				ready := true
				for _, replica := range info.Cluster.Replicas {
					ready = ready && replica.Current && !replica.Offline
				}
				if ready {
					done()
					return stream, nil
				}
			}
		}
		done()
		last = err
		select {
		case <-ctx.Done():
		case <-time.After(100 * time.Millisecond):
		}
	}
	return nil, fmt.Errorf("stream replica readiness: %v: %w", last, ctx.Err())
}
func (c *campaign) readers(ctx context.Context, js jetstream.JetStream, failures chan<- error, create bool) (func(), error) {
	readerCtx, cancel := context.WithCancel(ctx)
	var group sync.WaitGroup
	stop := func() { cancel(); group.Wait() }
	setupCtx, finishSetup := context.WithTimeout(ctx, 30*time.Second)
	defer finishSetup()
	for p := uint32(0); p < provision.Partitions; p++ {
		consumer, err := volumeConsumer(setupCtx, js, p, create)
		if err != nil {
			stop()
			return nil, err
		}
		group.Add(1)
		go func(p uint32, consumer jetstream.Consumer) {
			defer group.Done()
			fail := func(err error) {
				select {
				case failures <- err:
				default:
				}
				cancel()
			}
			for readerCtx.Err() == nil {
				batch, err := consumer.Fetch(128, jetstream.FetchMaxWait(time.Second))
				if err == nil {
					for msg := range batch.Messages() {
						if readerCtx.Err() != nil {
							break
						}
						if err := c.observe(msg, p); err != nil {
							fail(err)
							return
						}
						ackCtx, done := context.WithTimeout(readerCtx, 3*time.Second)
						err := msg.DoubleAck(ackCtx)
						done()
						if err != nil && readerCtx.Err() == nil {
							c.mu.Lock()
							c.rep.AckErrors++
							c.mu.Unlock()
						}
					}
					err = batch.Error()
				}
				if err != nil && readerCtx.Err() == nil && !errors.Is(err, nats.ErrTimeout) && !errors.Is(err, context.DeadlineExceeded) {
					c.mu.Lock()
					c.rep.FetchErrors++
					c.rep.LastFetchError = err.Error()
					c.mu.Unlock()
					select {
					case <-readerCtx.Done():
					case <-time.After(100 * time.Millisecond):
					}
				}
			}
		}(p, consumer)
	}
	return stop, nil
}
func run(cfg config) (runErr error) {
	if err := validate(cfg); err != nil {
		return err
	}
	if err := os.Mkdir(cfg.Root, 0755); err != nil {
		return fmt.Errorf("campaign needs a new root: %w", err)
	}
	c := &campaign{cfg: cfg, seen: make([]observation, cfg.Count)}
	c.rep = report{Storage: "file", Replicas: 3, Partitions: provision.Partitions, Publishers: cfg.Publishers, Lead: cfg.Lead.String(), Status: "running", GoVersion: runtime.Version(), Count: cfg.Count, Horizon: cfg.Horizon.String(), P99Limit: cfg.P99Limit.String(), MaxLateLimit: cfg.MaxLate.String()}
	if info, ok := debug.ReadBuildInfo(); ok {
		for _, setting := range info.Settings {
			if setting.Key == "vcs.revision" {
				c.rep.Revision = setting.Value
			}
			if setting.Key == "vcs.modified" {
				c.rep.SourceModified = setting.Value
			}
		}
	}
	defer func() {
		if err := c.saveObservations(); err != nil && runErr == nil {
			runErr = err
		}
		if runErr != nil {
			c.mu.Lock()
			c.rep.Status = "failed"
			c.rep.Error = runErr.Error()
			c.mu.Unlock()
		}
		if err := c.save(); runErr == nil && err != nil {
			runErr = err
		}
	}()
	cluster, err := testcluster.StartProcesses(filepath.Join(cfg.Root, "cluster"), 3)
	if err != nil {
		return err
	}
	defer cluster.Close()
	c.rep.ServerVersion = cluster.Clients[0].ConnectedServerVersion()
	ctx, cancel := context.WithTimeout(context.Background(), cfg.Lead+cfg.Horizon+5*time.Minute)
	defer cancel()
	js, err := jetstream.New(cluster.Clients[0])
	if err != nil {
		return err
	}
	readyCtx, done := context.WithTimeout(ctx, 45*time.Second)
	for readyCtx.Err() == nil {
		attempt, stopAttempt := context.WithTimeout(readyCtx, 3*time.Second)
		err = provision.Ensure(attempt, js, 3)
		stopAttempt()
		if err == nil {
			break
		}
		select {
		case <-readyCtx.Done():
		case <-time.After(100 * time.Millisecond):
		}
	}
	done()
	if err != nil {
		return fmt.Errorf("provision: %w", err)
	}
	readyCtx, done = context.WithTimeout(ctx, 45*time.Second)
	_, err = waitReady(readyCtx, js)
	done()
	if err != nil {
		return err
	}
	c.base = time.Now().UTC().Add(cfg.Lead)
	c.rep.FirstDue = c.base
	c.rep.LastDue = c.base.Add(cfg.Horizon)
	failures := make(chan error, 1)
	stopReaders, err := c.readers(ctx, js, failures, true)
	if err != nil {
		return err
	}
	defer func() {
		if stopReaders != nil {
			stopReaders()
		}
	}()
	if err := c.save(); err != nil {
		return err
	}
	loadCtx, stopLoad := context.WithDeadline(ctx, c.base)
	defer stopLoad()
	var publishers sync.WaitGroup
	jobs := make(chan int, cfg.Count)
	for i := 0; i < cfg.Count; i++ {
		jobs <- i
	}
	close(jobs)
	for range cfg.Publishers {
		publishers.Add(1)
		go func() {
			defer publishers.Done()
			port := worker.NewTimerSchedulePort(js)
			for index := range jobs {
				if loadCtx.Err() != nil {
					return
				}
				fresh, err := worker.ScheduleTimerWithPort(loadCtx, port, true, "volume", strconv.Itoa(index), uint64(index+1), 0, due(c.base, cfg.Horizon, cfg.Count, index))
				if err != nil || !fresh {
					select {
					case failures <- fmt.Errorf("schedule %d fresh=%v: %v", index, fresh, err):
					default:
					}
					stopLoad()
					return
				}
				c.mu.Lock()
				c.rep.Published++
				c.mu.Unlock()
			}
		}()
	}
	publishers.Wait()
	select {
	case err := <-failures:
		return err
	default:
	}
	if c.rep.Published != cfg.Count || !time.Now().Before(c.base) {
		return fmt.Errorf("load missed runway: acknowledged=%d/%d", c.rep.Published, cfg.Count)
	}
	if err := c.save(); err != nil {
		return err
	}
	fmt.Printf("loaded %d schedules; first due %s last due %s; root=%s\n", cfg.Count, c.rep.FirstDue, c.rep.LastDue, cfg.Root)
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	lastSave := time.Now()
	for {
		select {
		case err := <-failures:
			return err
		case <-ctx.Done():
			return fmt.Errorf("campaign deadline: %w", ctx.Err())
		case <-ticker.C:
		}
		now := time.Now()
		if len(c.rep.Restarts) < 2 && !now.Before(c.base.Add(cfg.Horizon*time.Duration(len(c.rep.Restarts)+1)/3)) {
			stopReaders()
			stopReaders = nil
			readyCtx, done := context.WithTimeout(ctx, 30*time.Second)
			_, err = waitReady(readyCtx, js)
			done()
			if err != nil {
				return err
			}
			c.mu.Lock()
			event := restart{Started: time.Now().UTC(), ReceivedBefore: c.rep.Received}
			c.mu.Unlock()
			c.rep.Restarts = append(c.rep.Restarts, event)
			if err = c.save(); err != nil {
				return err
			}
			for i := 0; i < 3; i++ {
				pid := cluster.Commands[i].Process.Pid
				if err = cluster.KillNode(i); err != nil {
					return err
				}
				status, ok := cluster.Commands[i].ProcessState.Sys().(syscall.WaitStatus)
				if !ok || !status.Signaled() || status.Signal() != syscall.SIGKILL {
					return fmt.Errorf("node %d did not exit by SIGKILL", i)
				}
				event.Killed = append(event.Killed, killedProcess{PID: pid, Signal: "SIGKILL"})
				c.rep.Restarts[len(c.rep.Restarts)-1] = event
			}
			for i := 0; i < 3; i++ {
				if err = cluster.RestartNode(i); err != nil {
					return err
				}
			}
			js, err = jetstream.New(cluster.Clients[0])
			if err != nil {
				return err
			}
			readyCtx, done = context.WithTimeout(ctx, 45*time.Second)
			_, err = waitReady(readyCtx, js)
			done()
			if err != nil {
				return err
			}
			stopReaders, err = c.readers(ctx, js, failures, false)
			if err != nil {
				return err
			}
			event.Healed = time.Now().UTC()
			c.rep.Restarts[len(c.rep.Restarts)-1] = event
			if err = c.save(); err != nil {
				return err
			}
			fmt.Printf("restart %d healed in %s after %d deliveries\n", len(c.rep.Restarts), event.Healed.Sub(event.Started), event.ReceivedBefore)
		}
		c.mu.Lock()
		received := c.rep.Received
		c.mu.Unlock()
		if received == cfg.Count && len(c.rep.Restarts) == 2 {
			attempt, done := context.WithTimeout(ctx, 3*time.Second)
			stream, err := js.Stream(attempt, "WF_RUN")
			var info *jetstream.StreamInfo
			if err == nil {
				info, err = stream.Info(attempt)
			}
			pending := 0
			if err == nil && info.State.Msgs == 0 {
				for p := uint32(0); p < provision.Partitions; p++ {
					consumer, e := js.Consumer(attempt, "WF_RUN", fmt.Sprintf("volume-%d", p))
					if e != nil {
						err = e
						break
					}
					ci, e := consumer.Info(attempt)
					if e != nil {
						err = e
						break
					}
					pending += ci.NumAckPending + int(ci.NumPending)
				}
			}
			done()
			if err == nil && info.State.Msgs == 0 && pending == 0 {
				stopReaders()
				stopReaders = nil
				c.rep.FinalMessages = info.State.Msgs
				c.rep.FinalAckPending = pending
				if err = c.save(); err != nil {
					return err
				}
				data, err := os.ReadFile(filepath.Join(cfg.Root, "report.json"))
				if err != nil {
					return err
				}
				var result report
				if err = json.Unmarshal(data, &result); err != nil {
					return err
				}
				if result.P99LateSeconds > cfg.P99Limit.Seconds() || result.MaxLateSeconds > cfg.MaxLate.Seconds() {
					return fmt.Errorf("lateness gate: p99=%gs max=%gs", result.P99LateSeconds, result.MaxLateSeconds)
				}
				c.rep.Status = "passed"
				fmt.Printf("all %d schedules delivered; p99=%gs max=%gs; queue drained\n", received, result.P99LateSeconds, result.MaxLateSeconds)
				return nil
			}
		}
		if time.Since(lastSave) >= time.Minute {
			if err = c.save(); err != nil {
				return err
			}
			lastSave = time.Now()
			fmt.Printf("progress received=%d/%d\n", received, cfg.Count)
		}
	}
}

// Attach retained consumers after restart; do not replace their configuration or
// mask a missing durable. Bound transport attempts independently of a 24h run.
func volumeConsumer(ctx context.Context, js jetstream.JetStream, partition uint32, create bool) (jetstream.Consumer, error) {
	name := fmt.Sprintf("volume-%d", partition)
	var last error
	for ctx.Err() == nil {
		attempt, done := context.WithTimeout(ctx, 3*time.Second)
		var consumer jetstream.Consumer
		var err error
		if create {
			consumer, err = js.CreateOrUpdateConsumer(attempt, "WF_RUN", jetstream.ConsumerConfig{Durable: name, FilterSubject: fmt.Sprintf("wf.run.%d", partition), AckPolicy: jetstream.AckExplicitPolicy, AckWait: 13 * time.Second, MaxAckPending: 1000, Replicas: 3})
		} else {
			consumer, err = js.Consumer(attempt, "WF_RUN", name)
		}
		done()
		if err == nil {
			return consumer, nil
		}
		last = err
		if !errors.Is(err, context.DeadlineExceeded) && !errors.Is(err, nats.ErrTimeout) && !errors.Is(err, nats.ErrNoResponders) {
			return nil, err
		}
		select {
		case <-ctx.Done():
		case <-time.After(100 * time.Millisecond):
		}
	}
	return nil, fmt.Errorf("consumer %s attachment: %v: %w", name, last, ctx.Err())
}

// One fixed-width record per scheduled index: little-endian stream sequence and
// first receipt Unix nanoseconds. Zero sequence denotes a missing delivery.
func (c *campaign) saveObservations() error {
	c.mu.Lock()
	data := make([]byte, 16*len(c.seen))
	for i, seen := range c.seen {
		binary.LittleEndian.PutUint64(data[16*i:], seen.Sequence)
		if seen.Sequence != 0 {
			binary.LittleEndian.PutUint64(data[16*i+8:], uint64(seen.At.UnixNano()))
		}
	}
	c.mu.Unlock()
	path := filepath.Join(c.cfg.Root, "observations.bin")
	if err := os.WriteFile(path+".tmp", data, 0644); err != nil {
		return err
	}
	if err := os.Rename(path+".tmp", path); err != nil {
		return err
	}
	sum := sha256.Sum256(data)
	c.rep.ObservationsSHA256 = hex.EncodeToString(sum[:])
	return nil
}
