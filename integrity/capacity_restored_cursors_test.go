//go:build linux

package integrity

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

// These exact identities were independently captured before/after the failed
// copied audit. This preparation applies only to a fresh full400k donor copy.
var capacityRestoredCursorCreated = map[string]string{
	"wf-audit-mweCABktZmjhL89Y7euEqi": "2026-10-05T19:54:54.24396315Z",
	"wf-audit-mweCABktZmjhL89Y7euF5i": "2026-10-05T19:55:14.447408717Z",
}

func validateRestoredCapacityCursors(initial []capacityConsumerInventory) ([]string, error) {
	if len(initial) != 2 {
		return nil, errors.New("restored preparation: require both inventories")
	}
	seen := map[string]bool{}
	var names []string
	for _, row := range initial {
		if seen[row.Stream] || row.Error != "" || row.NamesError != "" {
			return nil, errors.New("restored preparation: duplicate or unavailable inventory")
		}
		seen[row.Stream] = true
		if row.Stream == "WF_INV" {
			if row.Count != 0 || len(row.Names) != 0 || len(row.Consumers) != 0 {
				return nil, errors.New("restored preparation: unexpected INV consumer")
			}
			continue
		}
		if row.Stream != "WF_JRN" || row.Count != 2 || len(row.Names) != 2 || len(row.Consumers) != 2 {
			return nil, errors.New("restored preparation: unexpected JRN inventory")
		}
		listed := map[string]bool{}
		for _, name := range row.Names {
			if listed[name] || capacityRestoredCursorCreated[name] == "" {
				return nil, errors.New("restored preparation: unrecognized assignment name")
			}
			listed[name] = true
		}
		for _, info := range row.Consumers {
			if info == nil {
				return nil, errors.New("restored preparation: missing Info")
			}
			created := capacityRestoredCursorCreated[info.Name]
			cfg := info.Config
			if !listed[info.Name] || created == "" || info.Created.Format(time.RFC3339Nano) != created || info.Stream != "WF_JRN" || cfg.Name != info.Name || cfg.Replicas != 5 || !cfg.MemoryStorage || cfg.AckPolicy != jetstream.AckNonePolicy || cfg.DeliverPolicy != jetstream.DeliverByStartSequencePolicy || cfg.OptStartSeq != 1 || cfg.InactiveThreshold != 30*time.Second || cfg.FilterSubject != "" || len(cfg.FilterSubjects) != 0 || cfg.DeliverSubject != "" || cfg.Durable != "" || info.NumAckPending != 0 || info.NumRedelivered != 0 || info.Delivered.Consumer != 0 || info.Delivered.Stream != 0 || info.NumPending != 4800000 {
				return nil, fmt.Errorf("restored preparation: identity/config/position differs for %q", info.Name)
			}
			delete(listed, info.Name)
			names = append(names, info.Name)
		}
		if len(listed) != 0 {
			return nil, errors.New("restored preparation: unmatched names")
		}
	}
	if !seen["WF_INV"] || !seen["WF_JRN"] {
		return nil, errors.New("restored preparation: missing stream")
	}
	sort.Strings(names)
	return names, nil
}

type capacityPopulation struct {
	Stream   string `json:"stream"`
	Messages uint64 `json:"messages"`
	Bytes    uint64 `json:"bytes"`
	First    uint64 `json:"first"`
	Last     uint64 `json:"last"`
	Deleted  int    `json:"deleted"`
}

func capacityPopulations(ctx context.Context, js jetstream.JetStream) ([]capacityPopulation, error) {
	var rows []capacityPopulation
	for _, name := range []string{"WF_INV", "WF_JRN", "KV_WF_STATE"} {
		call, cancel := context.WithTimeout(ctx, time.Second)
		stream, err := js.Stream(call, name)
		cancel()
		if err != nil {
			return nil, err
		}
		state := stream.CachedInfo().State
		rows = append(rows, capacityPopulation{name, state.Msgs, state.Bytes, state.FirstSeq, state.LastSeq, state.NumDeleted})
	}
	return rows, nil
}

func prepareRestoredCapacityCursors(ctx context.Context, js jetstream.JetStream, root string, initial []capacityConsumerInventory) (failure error) {
	record := struct {
		Initial   []capacityConsumerInventory `json:"initial"`
		Before    []capacityPopulation        `json:"before"`
		After     []capacityPopulation        `json:"after"`
		Deletions []processDeleteObservation  `json:"deletions"`
		Final     []capacityConsumerInventory `json:"final"`
		Error     string                      `json:"error"`
	}{Initial: initial}
	defer func() {
		if failure != nil {
			record.Error = failure.Error()
		}
		data, err := json.MarshalIndent(record, "", "  ")
		if err == nil {
			err = os.WriteFile(filepath.Join(root, "restored-cursor-preparation.json"), append(data, '\n'), 0600)
		}
		failure = errors.Join(failure, err)
	}()
	names, err := validateRestoredCapacityCursors(initial)
	if err != nil {
		return err
	}
	record.Before, err = capacityPopulations(ctx, js)
	if err != nil {
		return err
	}
	stream, err := js.Stream(ctx, "WF_JRN")
	if err != nil {
		return err
	}
	for _, name := range names {
		call, cancel := context.WithTimeout(ctx, 2*time.Second)
		err := stream.DeleteConsumer(call, name)
		cancel()
		o := processDeleteObservation{Name: name, At: time.Now().UTC()}
		if err != nil {
			o.Error = err.Error()
		}
		record.Deletions = append(record.Deletions, o)
		if err != nil {
			return err
		}
	}
	for {
		var final []capacityConsumerInventory
		zero := true
		for _, name := range []string{"WF_INV", "WF_JRN"} {
			call, cancel := context.WithTimeout(ctx, time.Second)
			row := capacityReadConsumerInventory(call, js, name, nil)
			cancel()
			final = append(final, row)
			if row.Error != "" || row.Count != 0 || len(row.Names) != 0 || len(row.Consumers) != 0 {
				zero = false
			}
		}
		record.Final = final
		if zero {
			break
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(50 * time.Millisecond):
		}
	}
	record.After, err = capacityPopulations(ctx, js)
	if err != nil {
		return err
	}
	if !reflect.DeepEqual(record.Before, record.After) {
		return errors.New("restored preparation changed retained population boundaries")
	}
	return nil
}
