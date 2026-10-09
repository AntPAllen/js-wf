package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/signal"
	"plugin"
	"strings"
	"syscall"
	"time"

	"js-wf/assignment"
	"js-wf/identity"
	"js-wf/journal"
	"js-wf/provision"
	"js-wf/reconcile"
	"js-wf/retention"
	"js-wf/runtimeclock"
	"js-wf/worker"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := run(ctx, os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(ctx context.Context, args []string) error {
	return runWithJetStreamOptions(ctx, args)
}

func runWithJetStreamOptions(ctx context.Context, args []string, jsOptions ...jetstream.JetStreamOpt) (runErr error) {
	flags := flag.NewFlagSet("wf-worker", flag.ContinueOnError)
	url := flags.String("url", os.Getenv("NATS_URL"), "NATS server URL")
	domain := flags.String("domain", "", "JetStream domain (empty uses the default API)")
	id := flags.String("id", "", "unique worker ID")
	pluginPath := flags.String("handler-plugin", "", "Go plugin exporting a handler map")
	pluginSymbol := flags.String("handler-symbol", "Handlers", "plugin handler-map symbol")
	metricsAddr := flags.String("metrics-addr", "127.0.0.1:9090", "HTTP metrics listen address")
	replicas := flags.Int("replicas", 3, "JetStream stream replica count")
	journalMaxBytes := flags.Int64("journal-max-bytes", 0, "exact WF_JRN byte cap; zero adopts an uncapped stream")
	journalEncoding := flags.String("journal-encoding", "json", "new journal entry encoding: json or protobuf-v1; requires upgraded readers")
	graphAuthority := flags.String("graph-authority-stream", "", "experimental canonical graph authority stream (pre-provisioned)")
	graphPrefix := flags.String("graph-authority-prefix", "", "experimental canonical graph authority subject prefix")
	graphBucket := flags.String("graph-object-bucket", "", "experimental canonical graph object bucket (pre-provisioned)")
	timerBackend := flags.String("timer-backend", "auto", "timer storage mode: auto, native, or fallback")
	mode := flags.String("mode", "static", "partition assignment mode: static, kv, or auto")
	staticIndex := flags.Int("static-index", 0, "static worker index")
	staticCount := flags.Int("static-count", 1, "number of static workers")
	concurrency := flags.Int("partition-concurrency", 1, "concurrent deliveries per partition")
	retentionType := flags.String("retention-type", "", "optional workflow type for built-in durable purge handler")
	retentionGrace := flags.Duration("retention-grace", 24*time.Hour, "purge tombstone lifetime")
	eventPath := flags.String("events-file", "", "optional append-only JSONL fencing and repair event file")
	repair := flags.Bool("reconcile", true, "run leader-elected repair loops")
	clockFile := flags.String("timer-clock-config", "", "trusted independent clock topology JSON file")
	bootstrapClock := flags.Bool("provision-timer-clock", false, "create/verify clock probes before starting tagged timer writers")
	repairInterval := flags.Duration("reconcile-interval", time.Second, "repair scan cadence")
	repairBudget := flags.Int("reconcile-budget", 500, "stream sequences scanned per repair pass")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if flags.NArg() != 0 || *id == "" || *pluginPath == "" || *pluginSymbol == "" || *replicas < 1 || *replicas > 5 || *journalMaxBytes < 0 || *repairInterval <= 0 || *repairInterval > 10*time.Second || *repairBudget < 2 {
		return fmt.Errorf("usage: wf-worker -id ID -handler-plugin FILE [-domain NAME] [-mode static|kv|auto] [-metrics-addr ADDR]")
	}
	if journal.Encoding(*journalEncoding) != journal.JSON && journal.Encoding(*journalEncoding) != journal.ProtobufV1 {
		return fmt.Errorf("invalid journal encoding %q", *journalEncoding)
	}
	if *mode != "static" && *mode != "kv" && *mode != "auto" {
		return fmt.Errorf("invalid assignment mode %q", *mode)
	}
	if *timerBackend != "auto" && *timerBackend != "native" && *timerBackend != "fallback" {
		return fmt.Errorf("invalid timer backend %q", *timerBackend)
	}
	graphSelection, err := selectWorkerGraph(*graphAuthority, *graphPrefix, *graphBucket, *replicas, journal.Encoding(*journalEncoding), *timerBackend, *retentionType)
	if err != nil {
		return err
	}
	if *mode == "static" {
		if _, err := worker.StaticPartitions(*staticIndex, *staticCount); err != nil {
			return err
		}
	}
	if *retentionType != "" {
		if err := identity.ValidateToken(*retentionType); err != nil {
			return fmt.Errorf("retention workflow type: %w", err)
		}
		if *retentionGrace <= 0 {
			return fmt.Errorf("retention grace must be positive")
		}
	}
	var clockConfig *runtimeclock.Config
	if *bootstrapClock && *clockFile == "" {
		return fmt.Errorf("provision-timer-clock requires timer-clock-config")
	}
	if *clockFile != "" {
		if !*repair {
			return fmt.Errorf("timer-clock-config requires enabled repair loops")
		}
		file, err := os.Open(*clockFile)
		if err != nil {
			return fmt.Errorf("clock topology: %w", err)
		}
		cfg, readErr := runtimeclock.ReadConfig(file)
		closeErr := file.Close()
		if readErr != nil {
			return fmt.Errorf("clock topology: %w", readErr)
		}
		if closeErr != nil {
			return closeErr
		}
		clockConfig = &cfg
	}
	if *url == "" {
		*url = nats.DefaultURL
	}
	handlers, continuationOptions, err := loadHandlers(*pluginPath, *pluginSymbol)
	if err != nil {
		return err
	}
	if graphSelection == nil {
		continuationOptions = append(continuationOptions, worker.WithJournalEncoding(journal.Encoding(*journalEncoding)))
	}
	if *retentionType != "" {
		if _, exists := handlers[*retentionType]; exists {
			return fmt.Errorf("retention workflow type %q collides with plugin handler", *retentionType)
		}
	}
	var eventErrors <-chan error
	var observeRepair func(reconcile.RepairEvent)
	if *eventPath != "" {
		events, err := openWorkerEventLog(*eventPath, *id)
		if err != nil {
			return err
		}
		defer func() {
			if err := events.Close(); runErr == nil {
				runErr = err
			}
		}()
		continuationOptions = append(continuationOptions, worker.WithFencingObserver(events.fencing))
		observeRepair = events.repair
		eventErrors = events.errors
	}
	nc, js, backend, w, clock, err := startWorkerWithClockAndGraph(ctx, *url, *domain, *id, *replicas, *journalMaxBytes, *concurrency, *timerBackend, handlers, *retentionType, *retentionGrace, clockConfig, *bootstrapClock, jsOptions, graphSelection, continuationOptions...)
	if err != nil {
		if ctx.Err() != nil {
			return nil
		}
		return err
	}
	defer nc.Close()
	defer w.Close()
	var domainNow reconcile.TimerDomainClock
	if clock != nil {
		domainNow = clock.Lower
	}
	var controller *assignment.Controller
	if *mode == "auto" {
		startup, done := context.WithTimeout(ctx, 8*time.Second)
		members, err := assignment.EnsureMembership(startup, js, *replicas)
		if err == nil {
			var owners *assignment.Store
			owners, err = assignment.New(startup, js)
			if err == nil {
				controller, err = members.Controller(startup, *id, owners)
			}
		}
		done()
		if err != nil {
			return fmt.Errorf("automatic assignment startup: %w", err)
		}
		defer controller.Close()
	}
	listener, err := net.Listen("tcp", *metricsAddr)
	if err != nil {
		return fmt.Errorf("listen for worker metrics: %w", err)
	}
	mux := http.NewServeMux()
	mux.Handle("/metrics", metricsHandler(js, w))
	server := &http.Server{Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	serveDone := make(chan error, 1)
	go func() {
		err := server.Serve(listener)
		if errors.Is(err, http.ErrServerClosed) {
			err = nil
		}
		serveDone <- err
	}()
	served := false
	defer func() {
		shutdownCtx, stopShutdown := context.WithTimeout(context.Background(), 5*time.Second)
		defer stopShutdown()
		_ = server.Shutdown(shutdownCtx)
		if !served {
			<-serveDone
		}
	}()
	runCtx, stopRun := context.WithCancel(ctx)
	defer stopRun()
	results := make(chan error, 8)
	loops := 1
	if *mode == "auto" {
		go func() { results <- controller.RunWithWorkers(runCtx, w.RunKVAssignments, nil) }()
	} else if *mode == "kv" {
		go func() { results <- w.RunKVAssignments(runCtx) }()
	} else {
		go func() { results <- w.RunAssigned(runCtx, *staticIndex, *staticCount) }()
	}
	if *repair {
		start := []func(context.Context) error{
			func(c context.Context) error {
				return reconcile.RunRepairLoopWithClock(c, js, *id, "start", *repairInterval, *repairBudget, observeRepair, domainNow)
			},
			func(c context.Context) error {
				return reconcile.RunRepairLoopWithClock(c, js, *id, "signal", *repairInterval, *repairBudget, observeRepair, domainNow)
			},
			func(c context.Context) error {
				return reconcile.RunRepairLoopWithClock(c, js, *id, "timer", *repairInterval, *repairBudget, observeRepair, domainNow)
			},
			func(c context.Context) error {
				return reconcile.RunRepairLoopWithClock(c, js, *id, "suspended", *repairInterval, *repairBudget, observeRepair, domainNow)
			},
			func(c context.Context) error {
				return reconcile.RunTombstoneLoop(c, js, *id, *repairInterval, *repairBudget)
			},
		}
		if graphSelection != nil {
			start = graphWorkerRepairLoops(js, *id, *repairInterval, *repairBudget, graphSelection.store, observeRepair, domainNow)
		}
		if backend == provision.FallbackTimers {
			start = append(start, func(c context.Context) error {
				if graphSelection != nil {
					return reconcile.RunRepairLoopWithGraphJournal(c, js, *id, "fallback-timer", *repairInterval, *repairBudget, graphSelection.store, observeRepair, domainNow, nil)
				}
				return reconcile.RunRepairLoopWithClock(c, js, *id, "fallback-timer", *repairInterval, *repairBudget, observeRepair, domainNow)
			})
		}
		for _, loop := range start {
			loop := loop
			go func() { results <- loop(runCtx) }()
			loops++
		}
	}
	var firstErr error
	received := 0
	select {
	case <-ctx.Done():
	case err := <-eventErrors:
		firstErr = err
	case err := <-results:
		received = 1
		if ctx.Err() == nil {
			if err == nil || errors.Is(err, context.Canceled) {
				firstErr = fmt.Errorf("worker or repair loop stopped unexpectedly: %v", err)
			} else {
				firstErr = err
			}
		}
	case err := <-serveDone:
		served = true
		if err == nil && ctx.Err() == nil {
			firstErr = fmt.Errorf("metrics server stopped unexpectedly")
		} else if err != nil {
			firstErr = fmt.Errorf("metrics server: %w", err)
		}
	}
	stopRun()
	for i := received; i < loops; i++ {
		if err := <-results; err != nil && !errors.Is(err, context.Canceled) && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

func startWorker(ctx context.Context, url, id string, replicas int, journalMaxBytes int64, concurrency int, timerBackend string, handlers map[string]worker.Handler, retentionType string, retentionGrace time.Duration, options ...worker.Option) (*nats.Conn, jetstream.JetStream, provision.TimerBackend, *worker.Worker, error) {
	nc, js, backend, w, _, err := startWorkerWithClock(ctx, url, "", id, replicas, journalMaxBytes, concurrency, timerBackend, handlers, retentionType, retentionGrace, nil, false, nil, options...)
	return nc, js, backend, w, err
}

func startWorkerWithClock(ctx context.Context, url, domain, id string, replicas int, journalMaxBytes int64, concurrency int, timerBackend string, handlers map[string]worker.Handler, retentionType string, retentionGrace time.Duration, clockConfig *runtimeclock.Config, bootstrapClock bool, jsOptions []jetstream.JetStreamOpt, options ...worker.Option) (*nats.Conn, jetstream.JetStream, provision.TimerBackend, *worker.Worker, *runtimeclock.Clock, error) {
	return startWorkerWithClockAndGraph(ctx, url, domain, id, replicas, journalMaxBytes, concurrency, timerBackend, handlers, retentionType, retentionGrace, clockConfig, bootstrapClock, jsOptions, nil, options...)
}

func startWorkerWithClockAndGraph(ctx context.Context, url, domain, id string, replicas int, journalMaxBytes int64, concurrency int, timerBackend string, handlers map[string]worker.Handler, retentionType string, retentionGrace time.Duration, clockConfig *runtimeclock.Config, bootstrapClock bool, jsOptions []jetstream.JetStreamOpt, graph *workerGraphSelection, options ...worker.Option) (*nats.Conn, jetstream.JetStream, provision.TimerBackend, *worker.Worker, *runtimeclock.Clock, error) {
	startupCtx, stopStartup := context.WithTimeout(ctx, 30*time.Second)
	defer stopStartup()
	var lastErr error
	var clock *runtimeclock.Clock
	for startupCtx.Err() == nil {
		nc, err := nats.Connect(url, nats.Timeout(2*time.Second))
		if err == nil {
			var js jetstream.JetStream
			if domain == "" {
				js, err = jetstream.New(nc, jsOptions...)
			} else {
				js, err = jetstream.NewWithDomain(nc, domain, jsOptions...)
			}
			if err == nil {
				attempt, stop := context.WithTimeout(startupCtx, 5*time.Second)
				backend, provisionErr := ensureTimerBackend(attempt, js, replicas, journalMaxBytes, timerBackend)
				err = provisionErr
				stop()
				if err == nil {
					workerHandlers := make(map[string]worker.Handler, len(handlers)+1)
					for typ, handler := range handlers {
						workerHandlers[typ] = handler
					}
					if retentionType != "" {
						workerHandlers[retentionType] = retention.Handler(js, retentionGrace)
					}
					workerOptions := append(append([]worker.Option(nil), options...), worker.WithPartitionConcurrency(concurrency))
					if graph != nil {
						graph.store, err = journal.OpenNativeGraphStore(startupCtx, js, graph.config)
						if err == nil {
							workerOptions = append(workerOptions, worker.WithGraphJournal(graph.store))
						}
					}
					clock = nil
					if clockConfig != nil && err == nil {
						if bootstrapClock {
							err = runtimeclock.EnsureProbes(startupCtx, js, *clockConfig)
						}
						if err == nil {
							clock, err = runtimeclock.NewClock(js, *clockConfig)
						}
						if err == nil {
							_, _, err = clock.Bounds(startupCtx)
						}
						if err == nil {
							workerOptions = append(workerOptions, worker.WithTimerClock(runtimeclock.DeadlineDomain, clock.Bounds))
						}
					}
					var w *worker.Worker
					if err == nil {
						w, err = worker.New(startupCtx, js, id, workerHandlers, workerOptions...)
					}
					if err == nil {
						return nc, js, backend, w, clock, nil
					}
				}
			}
			nc.Close()
		}
		lastErr = err
		if !retryableStartupError(err) {
			return nil, nil, "", nil, nil, fmt.Errorf("start worker: %w", err)
		}
		select {
		case <-startupCtx.Done():
		case <-time.After(100 * time.Millisecond):
		}
	}
	if ctx.Err() != nil {
		return nil, nil, "", nil, nil, ctx.Err()
	}
	return nil, nil, "", nil, nil, fmt.Errorf("start worker after 30 seconds: %w", lastErr)
}

func ensureTimerBackend(ctx context.Context, js jetstream.JetStream, replicas int, journalMaxBytes int64, choice string) (provision.TimerBackend, error) {
	switch choice {
	case "auto":
		if journalMaxBytes > 0 {
			return provision.EnsureAutoWithJournalLimit(ctx, js, replicas, journalMaxBytes)
		}
		return provision.EnsureAuto(ctx, js, replicas)
	case "native":
		if journalMaxBytes > 0 {
			return provision.NativeTimers, provision.EnsureWithJournalLimit(ctx, js, replicas, journalMaxBytes)
		}
		return provision.NativeTimers, provision.Ensure(ctx, js, replicas)
	case "fallback":
		if journalMaxBytes > 0 {
			return provision.FallbackTimers, provision.EnsureFallbackWithJournalLimit(ctx, js, replicas, journalMaxBytes)
		}
		return provision.FallbackTimers, provision.EnsureFallback(ctx, js, replicas)
	default:
		return "", fmt.Errorf("invalid timer backend %q", choice)
	}
}

func retryableStartupError(err error) bool {
	var api *jetstream.APIError
	if errors.As(err, &api) && (api.ErrorCode == 10008 || api.ErrorCode == 10164) {
		return true
	}
	return errors.Is(err, jetstream.ErrNoStreamResponse) ||
		errors.Is(err, nats.ErrTimeout) || errors.Is(err, nats.ErrNoResponders) ||
		errors.Is(err, nats.ErrDisconnected) || errors.Is(err, nats.ErrConnectionReconnecting) || errors.Is(err, nats.ErrNoServers) ||
		errors.Is(err, context.DeadlineExceeded)
}

func metricsHandler(js jetstream.JetStream, w *worker.Worker) http.Handler {
	workerMetrics := worker.MetricsHandler(w)
	return http.HandlerFunc(func(out http.ResponseWriter, request *http.Request) {
		if request.Method != http.MethodGet {
			workerMetrics.ServeHTTP(out, request)
			return
		}
		query, stop := context.WithTimeout(request.Context(), 2*time.Second)
		defer stop()
		stream, err := js.Stream(query, "WF_JRN")
		if err != nil {
			http.Error(out, fmt.Sprintf("journal capacity lookup: %v", err), http.StatusServiceUnavailable)
			return
		}
		info, err := stream.Info(query)
		if err != nil {
			http.Error(out, fmt.Sprintf("journal capacity info: %v", err), http.StatusServiceUnavailable)
			return
		}
		workerMetrics.ServeHTTP(out, request)
		fmt.Fprintf(out, "# TYPE js_wf_journal_bytes gauge\njs_wf_journal_bytes %d\n", info.State.Bytes)
		fmt.Fprintf(out, "# TYPE js_wf_journal_limit_bytes gauge\njs_wf_journal_limit_bytes %d\n", info.Config.MaxBytes)
		if info.Config.MaxBytes > 0 {
			fmt.Fprintf(out, "# TYPE js_wf_journal_capacity_ratio gauge\njs_wf_journal_capacity_ratio %.9f\n", float64(info.State.Bytes)/float64(info.Config.MaxBytes))
		}
	})
}

func loadHandlers(path, symbolName string) (map[string]worker.Handler, []worker.Option, error) {
	loaded, err := plugin.Open(path)
	if err != nil {
		return nil, nil, fmt.Errorf("open handler plugin: %w", err)
	}
	symbol, err := loaded.Lookup(symbolName)
	if err != nil {
		return nil, nil, fmt.Errorf("load handler map %q: %w", symbolName, err)
	}
	var handlers map[string]worker.Handler
	var definitions map[string]worker.WorkflowDefinition
	var options []worker.Option
	switch value := symbol.(type) {
	case *map[string]worker.Handler:
		handlers = *value
	case func() map[string]worker.Handler:
		handlers = value()
	case *map[string]worker.WorkflowDefinition:
		definitions = *value
	case func() map[string]worker.WorkflowDefinition:
		definitions = value()
	default:
		return nil, nil, fmt.Errorf("plugin symbol %q must be a map[string]worker.Handler or map[string]worker.WorkflowDefinition, or a function returning either", symbolName)
	}
	if definitions != nil {
		handlers = make(map[string]worker.Handler, len(definitions))
		for typ, definition := range definitions {
			if err := definition.Validate(); err != nil {
				return nil, nil, fmt.Errorf("workflow %q: %w", typ, err)
			}
			handlers[typ] = definition.Handler
			if len(definition.Continuations) != 0 {
				options = append(options, worker.WithContinuations(typ, definition.Continuations))
			}
		}
	}
	if len(handlers) == 0 {
		return nil, nil, fmt.Errorf("handler map is empty")
	}
	for typ, handler := range handlers {
		if strings.TrimSpace(typ) != typ || typ == "" || handler == nil {
			return nil, nil, fmt.Errorf("invalid handler for type %q", typ)
		}
	}
	return handlers, options, nil
}
