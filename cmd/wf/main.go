package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"js-wf/assignment"
	"js-wf/client"
	"js-wf/provision"
	"js-wf/reconcile"
	"js-wf/retention"
	"js-wf/visibility"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

func main() {
	if err := run(os.Args[1:], os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(args []string, out io.Writer) error {
	flags := flag.NewFlagSet("wf", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	url := flags.String("url", os.Getenv("NATS_URL"), "NATS server URL")
	timeout := flags.Duration("timeout", 2*time.Minute, "operation timeout")
	grace := flags.Duration("grace", 24*time.Hour, "purge tombstone grace")
	rebuild := flags.Bool("rebuild", false, "rebuild the visibility view before listing")
	attribute := flags.String("attribute", "", "filter list by a search attribute (key=value)")
	postgresDSN := flags.String("postgres-dsn", os.Getenv("WF_POSTGRES_DSN"), "PostgreSQL visibility connection string")
	cursor := flags.Uint64("cursor", 1, "scan starting stream sequence")
	budget := flags.Int("budget", 100, "scan sequence budget")
	interval := flags.Duration("interval", 100*time.Millisecond, "reconciler loop interval")
	scanGrace := flags.Duration("scan-grace", time.Second, "overdue timer grace for suspended scan")
	apply := flags.Bool("apply", false, "apply scan actions")
	replayPlugin := flags.String("handler-plugin", "", "Go plugin exporting a replay handler")
	replaySymbol := flags.String("handler-symbol", "Workflow", "handler symbol in the replay plugin")
	replayBundlePath := flags.String("replay-bundle", "", "offline replay bundle JSON path")
	if err := flags.Parse(args); err != nil {
		return err
	}
	command := flags.Args()
	if len(command) == 0 {
		return errors.New("usage: wf [-url nats://...] [-attribute key=value] {project|list [status]|describe type id|lag|export-journal type id|export-replay type id|replay type id|cancel type id|purge type id|sweep-tombstones|scan-tombstones|tombstone-loop|scan-suspended|journal-capacity|assignment-init worker...|assignment-get partition|assignment-move partition owner revision}")
	}
	encode := func(value any) error {
		writer := json.NewEncoder(out)
		writer.SetIndent("", "  ")
		return writer.Encode(value)
	}
	if command[0] == "replay" && *replayBundlePath != "" {
		if len(command) != 1 || *replayPlugin == "" || *replaySymbol == "" {
			return errors.New("usage: wf -handler-plugin path -replay-bundle file replay")
		}
		bundle, err := loadReplayBundle(*replayBundlePath)
		if err != nil {
			return err
		}
		report, err := runReplayBundle(bundle, *replayPlugin, *replaySymbol)
		if err != nil {
			return err
		}
		return encode(report)
	}
	if *url == "" {
		*url = nats.DefaultURL
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
	var projectionOptions []visibility.Option
	if *postgresDSN != "" {
		db, err := sql.Open("pgx", *postgresDSN)
		if err != nil {
			return err
		}
		defer db.Close()
		projectionOptions = append(projectionOptions, visibility.WithPostgres(&visibility.PostgresStore{DB: db}))
	}
	if command[0] == "project" {
		if len(command) != 1 {
			return errors.New("usage: wf project")
		}
		ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
		defer stop()
		projection, err := visibility.New(ctx, js, projectionOptions...)
		if err != nil {
			return err
		}
		return projection.Run(ctx)
	}
	if command[0] == "tombstone-loop" {
		if len(command) != 1 {
			return errors.New("usage: wf [-interval duration] [-budget n] tombstone-loop")
		}
		ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
		defer stop()
		return reconcile.RunTombstoneLoop(ctx, js, fmt.Sprintf("tombstone-%d", os.Getpid()), *interval, *budget)
	}
	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()
	switch command[0] {
	case "export-replay":
		if len(command) != 3 {
			return errors.New("usage: wf export-replay type id")
		}
		bundle, err := fetchReplayBundle(ctx, js, command[1], command[2])
		if err != nil {
			return err
		}
		return encode(bundle)
	case "replay":
		if len(command) != 3 || *replayPlugin == "" || *replaySymbol == "" {
			return errors.New("usage: wf -handler-plugin path [-handler-symbol Workflow] replay type id")
		}
		report, err := replayInvocation(ctx, js, command[1], command[2], *replayPlugin, *replaySymbol)
		if err != nil {
			return err
		}
		return encode(report)
	case "assignment-init":
		if len(command) < 2 {
			return errors.New("usage: wf assignment-init worker...")
		}
		store, err := assignment.New(ctx, js)
		if err != nil {
			return err
		}
		if err := store.InitializeStatic(ctx, command[1:]); err != nil {
			return err
		}
		return encode(map[string]any{"initialized": true, "workers": command[1:], "partitions": provision.Partitions})
	case "assignment-get", "assignment-move":
		if command[0] == "assignment-get" && len(command) != 2 {
			return errors.New("usage: wf assignment-get partition")
		}
		if command[0] == "assignment-move" && len(command) != 4 {
			return errors.New("usage: wf assignment-move partition owner revision")
		}
		partition, err := strconv.ParseUint(command[1], 10, 32)
		if err != nil || partition >= uint64(provision.Partitions) {
			return fmt.Errorf("invalid partition %q", command[1])
		}
		store, err := assignment.New(ctx, js)
		if err != nil {
			return err
		}
		if command[0] == "assignment-get" {
			owner, revision, err := store.Get(ctx, uint32(partition))
			if err != nil {
				return err
			}
			return encode(map[string]any{"partition": partition, "owner": owner, "revision": revision})
		}
		expected, err := strconv.ParseUint(command[3], 10, 64)
		if err != nil {
			return fmt.Errorf("invalid assignment revision %q", command[3])
		}
		revision, err := store.Assign(ctx, uint32(partition), command[2], expected)
		if err != nil {
			return err
		}
		return encode(map[string]any{"partition": partition, "owner": command[2], "revision": revision})
	case "journal-capacity":
		if len(command) != 1 {
			return errors.New("usage: wf journal-capacity")
		}
		capacity, err := provision.CheckJournalCapacity(ctx, js)
		if err != nil {
			return err
		}
		if err := encode(capacity); err != nil {
			return err
		}
		if capacity.Alert {
			return fmt.Errorf("WF_JRN at %.1f%% of byte limit (70%% alert threshold)", 100*capacity.Utilization)
		}
		return nil
	case "cancel":
		if len(command) != 3 {
			return errors.New("usage: wf cancel type id")
		}
		sequence, err := client.New(js).Cancel(ctx, command[1], command[2])
		if err != nil {
			return err
		}
		return encode(map[string]any{"requested": true, "type": command[1], "id": command[2], "signal_seq": sequence})
	case "scan-suspended":
		if len(command) != 1 {
			return errors.New("usage: wf [-cursor n] [-budget n] [-scan-grace duration] [-apply] scan-suspended")
		}
		scan := reconcile.NewSuspendedScan(js)
		scan.Grace = *scanGrace
		result, err := scan.Scan(ctx, *cursor, *budget, !*apply)
		if err != nil {
			return err
		}
		return encode(result)
	case "sweep-tombstones":
		if len(command) != 1 {
			return errors.New("usage: wf sweep-tombstones")
		}
		result, err := retention.SweepTombstones(ctx, js, time.Now().UTC())
		if err != nil {
			return err
		}
		return encode(result)
	case "scan-tombstones":
		if len(command) != 1 {
			return errors.New("usage: wf [-cursor n] [-budget n] [-apply] scan-tombstones")
		}
		page, err := retention.NewTombstoneScan(js).Scan(ctx, *cursor, *budget, time.Now().UTC(), !*apply)
		if err != nil {
			return err
		}
		return encode(page)
	case "purge":
		if len(command) != 3 {
			return errors.New("usage: wf purge type id")
		}
		if err := retention.Purge(ctx, js, command[1], command[2], *grace); err != nil {
			return err
		}
		return encode(map[string]any{"purged": true, "type": command[1], "id": command[2]})
	case "list", "describe", "lag", "export-journal":
		projection, err := visibility.New(ctx, js, projectionOptions...)
		if err != nil {
			return err
		}
		switch command[0] {
		case "list":
			if len(command) > 2 {
				return errors.New("usage: wf list [status]")
			}
			if *rebuild {
				if err := projection.Rebuild(ctx); err != nil {
					return err
				}
			}
			status := ""
			if len(command) == 2 {
				status = command[1]
			}
			var rows []visibility.Row
			var err error
			if *attribute != "" {
				key, value, found := strings.Cut(*attribute, "=")
				if !found {
					return errors.New("attribute filter must be key=value")
				}
				rows, err = projection.ListByAttribute(ctx, key, value, status)
			} else {
				rows, err = projection.List(ctx, status)
			}
			if err != nil {
				return err
			}
			return encode(rows)
		case "describe":
			if len(command) != 3 {
				return errors.New("usage: wf describe type id")
			}
			row, records, err := projection.Describe(ctx, command[1], command[2])
			if err != nil {
				return err
			}
			return encode(struct {
				Row     visibility.Row `json:"row"`
				Journal any            `json:"journal"`
			}{row, records})
		case "lag":
			if len(command) != 1 {
				return errors.New("usage: wf lag")
			}
			lag, err := projection.Lag(ctx)
			if err != nil {
				return err
			}
			return encode(map[string]uint64{"pending": lag})
		case "export-journal":
			if len(command) != 3 {
				return errors.New("usage: wf export-journal type id")
			}
			_, records, err := projection.Describe(ctx, command[1], command[2])
			if err != nil {
				return err
			}
			return encode(records)
		}
	}
	return fmt.Errorf("unknown command %q", command[0])
}
