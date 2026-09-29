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

	"js-wf/provision"
	"js-wf/reconcile"
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
	flags := flag.NewFlagSet("wf-worker", flag.ContinueOnError)
	url := flags.String("url", os.Getenv("NATS_URL"), "NATS server URL")
	id := flags.String("id", "", "unique worker ID")
	pluginPath := flags.String("handler-plugin", "", "Go plugin exporting a handler map")
	pluginSymbol := flags.String("handler-symbol", "Handlers", "plugin handler-map symbol")
	metricsAddr := flags.String("metrics-addr", "127.0.0.1:9090", "HTTP metrics listen address")
	replicas := flags.Int("replicas", 3, "JetStream stream replica count")
	mode := flags.String("mode", "static", "partition assignment mode: static or kv")
	staticIndex := flags.Int("static-index", 0, "static worker index")
	staticCount := flags.Int("static-count", 1, "number of static workers")
	concurrency := flags.Int("partition-concurrency", 1, "concurrent deliveries per partition")
	repair := flags.Bool("reconcile", true, "run leader-elected repair loops")
	repairInterval := flags.Duration("reconcile-interval", time.Second, "repair scan cadence")
	repairBudget := flags.Int("reconcile-budget", 500, "stream sequences scanned per repair pass")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if flags.NArg() != 0 || *id == "" || *pluginPath == "" || *pluginSymbol == "" || *replicas < 1 || *replicas > 3 || *repairInterval <= 0 || *repairBudget < 1 {
		return fmt.Errorf("usage: wf-worker -id ID -handler-plugin FILE [-mode static|kv] [-metrics-addr ADDR]")
	}
	if *mode != "static" && *mode != "kv" {
		return fmt.Errorf("invalid assignment mode %q", *mode)
	}
	if *mode == "static" {
		if _, err := worker.StaticPartitions(*staticIndex, *staticCount); err != nil {
			return err
		}
	}
	if *url == "" {
		*url = nats.DefaultURL
	}
	handlers, err := loadHandlers(*pluginPath, *pluginSymbol)
	if err != nil {
		return err
	}
	nc, err := nats.Connect(*url)
	if err != nil {
		return err
	}
	defer nc.Close()
	js, err := jetstream.New(nc)
	if err != nil {
		return err
	}
	setupCtx, stopSetup := context.WithTimeout(ctx, 30*time.Second)
	backend, err := provision.EnsureAuto(setupCtx, js, *replicas)
	stopSetup()
	if err != nil {
		return fmt.Errorf("provision workflow stores: %w", err)
	}
	w, err := worker.New(ctx, js, *id, handlers, worker.WithPartitionConcurrency(*concurrency))
	if err != nil {
		return err
	}
	listener, err := net.Listen("tcp", *metricsAddr)
	if err != nil {
		return fmt.Errorf("listen for worker metrics: %w", err)
	}
	mux := http.NewServeMux()
	mux.Handle("/metrics", worker.MetricsHandler(w))
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
	results := make(chan error, 6)
	loops := 1
	if *mode == "kv" {
		go func() { results <- w.RunKVAssignments(runCtx) }()
	} else {
		go func() { results <- w.RunAssigned(runCtx, *staticIndex, *staticCount) }()
	}
	if *repair {
		start := []func(context.Context) error{
			func(c context.Context) error {
				return reconcile.RunStartLoop(c, js, *id, *repairInterval, *repairBudget)
			},
			func(c context.Context) error {
				return reconcile.RunSignalLoop(c, js, *id, *repairInterval, *repairBudget)
			},
			func(c context.Context) error {
				return reconcile.RunTimerLoop(c, js, *id, *repairInterval, *repairBudget)
			},
			func(c context.Context) error {
				return reconcile.RunSuspendedLoop(c, js, *id, *repairInterval, *repairBudget)
			},
		}
		if backend == provision.FallbackTimers {
			start = append(start, func(c context.Context) error {
				return reconcile.RunFallbackTimerLoop(c, js, *id, *repairInterval, *repairBudget)
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

func loadHandlers(path, symbolName string) (map[string]worker.Handler, error) {
	loaded, err := plugin.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open handler plugin: %w", err)
	}
	symbol, err := loaded.Lookup(symbolName)
	if err != nil {
		return nil, fmt.Errorf("load handler map %q: %w", symbolName, err)
	}
	var handlers map[string]worker.Handler
	switch value := symbol.(type) {
	case *map[string]worker.Handler:
		handlers = *value
	case func() map[string]worker.Handler:
		handlers = value()
	default:
		return nil, fmt.Errorf("plugin symbol %q must be a map[string]worker.Handler or a function returning one", symbolName)
	}
	if len(handlers) == 0 {
		return nil, fmt.Errorf("handler map is empty")
	}
	for typ, handler := range handlers {
		if strings.TrimSpace(typ) != typ || typ == "" || handler == nil {
			return nil, fmt.Errorf("invalid handler for type %q", typ)
		}
	}
	return handlers, nil
}
