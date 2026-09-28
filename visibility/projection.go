// Package visibility maintains a rebuildable, small-deployment query view.
package visibility

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"js-wf/identity"
	"js-wf/journal"
	"js-wf/wf"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

var ErrNotFound = errors.New("invocation not found")

type Row struct {
	SchemaVersion int               `json:"schema_version"`
	Type          string            `json:"type"`
	ID            string            `json:"id"`
	Status        string            `json:"status"`
	Started       time.Time         `json:"started"`
	Updated       time.Time         `json:"updated"`
	WaitingOn     string            `json:"waiting_on,omitempty"`
	InvSeq        uint64            `json:"inv_seq"`
	JournalSeq    uint64            `json:"journal_seq,omitempty"`
	LastIndex     uint64            `json:"last_index,omitempty"`
	Attributes    map[string]string `json:"attributes,omitempty"`
}

type Projection struct {
	js              jetstream.JetStream
	inv             jetstream.Stream
	jrn             jetstream.Stream
	view            jetstream.KeyValue
	consumer        jetstream.Consumer
	schemaVersion   int
	attributeMapper func(map[string]string) (map[string]string, error)
}

type Option func(*Projection) error

// WithAttributeMapper projects older journaled attribute shapes into a new
// schema. Deploy one version at a time and run Rebuild to migrate every row.
func WithAttributeMapper(version int, mapper func(map[string]string) (map[string]string, error)) Option {
	return func(p *Projection) error {
		if version < 2 || mapper == nil {
			return fmt.Errorf("attribute mapper requires schema version >= 2 and a mapper")
		}
		p.schemaVersion = version
		p.attributeMapper = mapper
		return nil
	}
}

func New(ctx context.Context, js jetstream.JetStream, options ...Option) (*Projection, error) {
	p := &Projection{js: js, schemaVersion: 1}
	for _, option := range options {
		if err := option(p); err != nil {
			return nil, err
		}
	}
	inv, err := js.Stream(ctx, "WF_INV")
	if err != nil {
		return nil, err
	}
	jrn, err := js.Stream(ctx, "WF_JRN")
	if err != nil {
		return nil, err
	}
	view, err := js.KeyValue(ctx, "WF_VIEW")
	if err != nil {
		return nil, err
	}
	consumer, err := jrn.CreateConsumer(ctx, jetstream.ConsumerConfig{
		Name: "WF_VIEW", Durable: "WF_VIEW", FilterSubject: "wf.jrn.*.*",
		AckPolicy: jetstream.AckExplicitPolicy, AckWait: 30 * time.Second, MaxDeliver: -1,
	})
	if err != nil {
		return nil, err
	}
	p.inv, p.jrn, p.view, p.consumer = inv, jrn, view, consumer
	return p, nil
}

func rowKey(typ, id string) string { return "row." + identity.Key(typ, id) }
func indexKey(row Row) string      { return "idx." + row.Status + "." + identity.Key(row.Type, row.ID) }

func attributeIndexKey(row Row, key, value string) string {
	digest := sha256.Sum256([]byte(value))
	return "idxa." + key + "." + hex.EncodeToString(digest[:]) + "." + identity.Key(row.Type, row.ID)
}

func attributeIndexKeys(row Row) []string {
	keys := make([]string, 0, len(row.Attributes))
	for key, value := range row.Attributes {
		keys = append(keys, attributeIndexKey(row, key, value))
	}
	return keys
}

func projectedAttributes(records []journal.Record) (map[string]string, error) {
	var attributes map[string]string
	for i := 0; i < len(records); i++ {
		if records[i].Kind != journal.StepRequested {
			continue
		}
		var request struct {
			Kind string `json:"kind"`
		}
		if err := json.Unmarshal(records[i].Payload, &request); err != nil {
			return nil, fmt.Errorf("decode step request %d: %w", records[i].Index, err)
		}
		if request.Kind != "search_attributes" || i+1 >= len(records) || records[i+1].Kind != journal.StepCompleted {
			continue
		}
		var done struct {
			Result json.RawMessage `json:"result"`
			Error  string          `json:"error"`
			Ref    string          `json:"result_ref"`
		}
		if err := json.Unmarshal(records[i+1].Payload, &done); err != nil {
			return nil, fmt.Errorf("decode search attributes at %d: %w", records[i+1].Index, err)
		}
		if done.Error != "" {
			continue
		}
		var replacement map[string]string
		if done.Ref != "" || len(done.Result) == 0 || json.Unmarshal(done.Result, &replacement) != nil || wf.ValidateSearchAttributes(replacement) != nil {
			return nil, fmt.Errorf("invalid search attributes at journal index %d", records[i+1].Index)
		}
		attributes = replacement
	}
	return attributes, nil
}

func (p *Projection) describe(ctx context.Context, input *jetstream.RawStreamMsg) (Row, []journal.Record, error) {
	parts := strings.Split(input.Subject, ".")
	if len(parts) != 4 || parts[0] != "wf" || parts[1] != "inv" || identity.Validate(parts[2], parts[3]) != nil {
		return Row{}, nil, fmt.Errorf("invalid invocation subject %q", input.Subject)
	}
	typ, id := parts[2], parts[3]
	records, tail, err := journal.New(p.js).Read(ctx, typ, id)
	if err != nil {
		return Row{}, nil, err
	}
	row := Row{SchemaVersion: p.schemaVersion, Type: typ, ID: id, Status: "queued", Started: input.Time, Updated: input.Time, InvSeq: input.Sequence, JournalSeq: tail}
	row.Attributes, err = projectedAttributes(records)
	if err != nil {
		return Row{}, nil, err
	}
	if p.attributeMapper != nil {
		row.Attributes, err = p.attributeMapper(row.Attributes)
		if err != nil {
			return Row{}, nil, fmt.Errorf("map search attributes: %w", err)
		}
		if err := wf.ValidateSearchAttributes(row.Attributes); err != nil {
			return Row{}, nil, fmt.Errorf("mapped search attributes: %w", err)
		}
	}
	if len(records) == 0 {
		return row, records, nil
	}
	last := records[len(records)-1]
	row.LastIndex = last.Index
	switch last.Kind {
	case journal.Completed:
		row.Status = "completed"
	case journal.Failed:
		row.Status = "failed"
	case journal.Suspended:
		row.Status = "suspended"
		var wait struct {
			WaitingOn string `json:"waiting_on"`
		}
		if err := json.Unmarshal(last.Payload, &wait); err != nil {
			return Row{}, nil, err
		}
		row.WaitingOn = wait.WaitingOn
	default:
		row.Status = "running"
	}
	if tail != 0 {
		if msg, err := p.jrn.GetMsg(ctx, tail); err == nil {
			row.Updated = msg.Time
		} else if !errors.Is(err, jetstream.ErrMsgNotFound) {
			return Row{}, nil, err
		}
	}
	return row, records, nil
}

// Describe reads the source of truth, including a compacted snapshot prefix.
func (p *Projection) Describe(ctx context.Context, typ, id string) (Row, []journal.Record, error) {
	if err := identity.Validate(typ, id); err != nil {
		return Row{}, nil, err
	}
	input, err := p.inv.GetLastMsgForSubject(ctx, identity.InvocationSubject(typ, id))
	if errors.Is(err, jetstream.ErrMsgNotFound) {
		return Row{}, nil, ErrNotFound
	}
	if err != nil {
		return Row{}, nil, err
	}
	return p.describe(ctx, input)
}

// SyncOne projects the latest logical journal or removes a purged invocation.
func (p *Projection) SyncOne(ctx context.Context, typ, id string) error {
	row, _, err := p.Describe(ctx, typ, id)
	if errors.Is(err, ErrNotFound) {
		return p.deleteRow(ctx, typ, id)
	}
	if err != nil {
		return err
	}
	return p.putRow(ctx, row)
}

func (p *Projection) putRow(ctx context.Context, row Row) error {
	key := rowKey(row.Type, row.ID)
	data, err := json.Marshal(row)
	if err != nil {
		return err
	}
	prior, err := p.view.Get(ctx, key)
	if err != nil && !errors.Is(err, jetstream.ErrKeyNotFound) {
		return err
	}
	if err == nil && bytes.Equal(prior.Value(), data) {
		return p.ensureIndexes(ctx, row)
	}
	if _, err := p.view.Put(ctx, key, data); err != nil {
		return err
	}
	if prior != nil {
		var previous Row
		if json.Unmarshal(prior.Value(), &previous) != nil {
			return fmt.Errorf("invalid projection row %s", key)
		}
		if previous.Status != row.Status {
			if err := p.view.Delete(ctx, indexKey(previous)); err != nil && !errors.Is(err, jetstream.ErrKeyNotFound) {
				return err
			}
		}
		for key, value := range previous.Attributes {
			if row.Attributes[key] == value {
				if _, exists := row.Attributes[key]; exists {
					continue
				}
			}
			if err := p.view.Delete(ctx, attributeIndexKey(previous, key, value)); err != nil && !errors.Is(err, jetstream.ErrKeyNotFound) {
				return err
			}
		}
	}
	return p.ensureIndexes(ctx, row)
}

func (p *Projection) ensureIndexes(ctx context.Context, row Row) error {
	for _, key := range append([]string{indexKey(row)}, attributeIndexKeys(row)...) {
		if err := p.ensureIndex(ctx, key, row.InvSeq); err != nil {
			return err
		}
	}
	return nil
}

func (p *Projection) ensureIndex(ctx context.Context, key string, invSeq uint64) error {
	value := []byte(strconv.FormatUint(invSeq, 10))
	prior, err := p.view.Get(ctx, key)
	if err == nil && bytes.Equal(prior.Value(), value) {
		return nil
	}
	if err != nil && !errors.Is(err, jetstream.ErrKeyNotFound) {
		return err
	}
	_, err = p.view.Put(ctx, key, value)
	return err
}

func (p *Projection) deleteRow(ctx context.Context, typ, id string) error {
	key := rowKey(typ, id)
	prior, err := p.view.Get(ctx, key)
	if errors.Is(err, jetstream.ErrKeyNotFound) {
		return nil
	}
	if err != nil {
		return err
	}
	var row Row
	if err := json.Unmarshal(prior.Value(), &row); err != nil {
		return err
	}
	if err := p.view.Delete(ctx, indexKey(row)); err != nil && !errors.Is(err, jetstream.ErrKeyNotFound) {
		return err
	}
	for _, attributeKey := range attributeIndexKeys(row) {
		if err := p.view.Delete(ctx, attributeKey); err != nil && !errors.Is(err, jetstream.ErrKeyNotFound) {
			return err
		}
	}
	return p.view.Delete(ctx, key)
}

// Rebuild reconciles all rows against retained invocations and logical
// journals. It also removes rows and index entries for purged invocations.
func (p *Projection) Rebuild(ctx context.Context) error {
	info, err := p.inv.Info(ctx)
	if err != nil {
		return err
	}
	wanted := map[string]struct{}{}
	// Every invocation has independent row and index keys, so bounded workers
	// can rebuild them concurrently without changing their final bytes.
	rebuildCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	jobs := make(chan uint64, 64)
	var workers sync.WaitGroup
	var wantedMu sync.Mutex
	var firstErr error
	var failOnce sync.Once
	fail := func(err error) {
		failOnce.Do(func() {
			firstErr = err
			cancel()
		})
	}
	for i := 0; i < 32; i++ {
		workers.Add(1)
		go func() {
			defer workers.Done()
			for seq := range jobs {
				input, err := p.inv.GetMsg(rebuildCtx, seq)
				if errors.Is(err, jetstream.ErrMsgNotFound) {
					continue
				}
				if err != nil {
					fail(err)
					return
				}
				row, _, err := p.describe(rebuildCtx, input)
				if err == nil {
					err = p.putRow(rebuildCtx, row)
				}
				if err != nil {
					fail(err)
					return
				}
				wantedMu.Lock()
				wanted[rowKey(row.Type, row.ID)] = struct{}{}
				wanted[indexKey(row)] = struct{}{}
				for _, key := range attributeIndexKeys(row) {
					wanted[key] = struct{}{}
				}
				wantedMu.Unlock()
			}
		}()
	}
produce:
	for seq := info.State.FirstSeq; seq != 0 && seq <= info.State.LastSeq; seq++ {
		select {
		case jobs <- seq:
		case <-rebuildCtx.Done():
			break produce
		}
	}
	close(jobs)
	workers.Wait()
	if firstErr != nil {
		return firstErr
	}
	if ctx.Err() != nil {
		return ctx.Err()
	}
	keys, err := p.view.Keys(ctx)
	if err != nil && !errors.Is(err, jetstream.ErrNoKeysFound) {
		return err
	}
	for _, key := range keys {
		if _, ok := wanted[key]; ok {
			continue
		}
		if strings.HasPrefix(key, "row.") || strings.HasPrefix(key, "idx.") || strings.HasPrefix(key, "idxa.") {
			if err := p.view.Delete(ctx, key); err != nil && !errors.Is(err, jetstream.ErrKeyNotFound) {
				return err
			}
		}
	}
	return nil
}

func (p *Projection) Get(ctx context.Context, typ, id string) (Row, error) {
	if err := identity.Validate(typ, id); err != nil {
		return Row{}, err
	}
	value, err := p.view.Get(ctx, rowKey(typ, id))
	if errors.Is(err, jetstream.ErrKeyNotFound) {
		return Row{}, ErrNotFound
	}
	if err != nil {
		return Row{}, err
	}
	var row Row
	if err := json.Unmarshal(value.Value(), &row); err != nil {
		return Row{}, err
	}
	return row, nil
}

func (p *Projection) List(ctx context.Context, status string) ([]Row, error) {
	if status != "" && identity.ValidateToken(status) != nil {
		return nil, fmt.Errorf("invalid status %q", status)
	}
	keys, err := p.view.Keys(ctx)
	if errors.Is(err, jetstream.ErrNoKeysFound) {
		return []Row{}, nil
	}
	if err != nil {
		return nil, err
	}
	rows := make([]Row, 0)
	prefix := "row."
	if status != "" {
		prefix = "idx." + status + "."
	}
	for _, key := range keys {
		if !strings.HasPrefix(key, prefix) {
			continue
		}
		rowKey := key
		if status != "" {
			rowKey = "row." + strings.TrimPrefix(key, prefix)
		}
		value, err := p.view.Get(ctx, rowKey)
		if errors.Is(err, jetstream.ErrKeyNotFound) {
			continue
		}
		if err != nil {
			return nil, err
		}
		var row Row
		if err := json.Unmarshal(value.Value(), &row); err != nil {
			return nil, err
		}
		if status == "" || row.Status == status && indexKey(row) == key {
			rows = append(rows, row)
		}
	}
	sort.Slice(rows, func(i, j int) bool {
		if rows[i].Type == rows[j].Type {
			return rows[i].ID < rows[j].ID
		}
		return rows[i].Type < rows[j].Type
	})
	return rows, nil
}

// ListByAttribute uses the value-hashed KV index and verifies each row before
// returning it, so a stale index cannot produce a false match.
func (p *Projection) ListByAttribute(ctx context.Context, key, value, status string) ([]Row, error) {
	if err := wf.ValidateSearchAttributes(map[string]string{key: value}); err != nil {
		return nil, err
	}
	if status != "" && identity.ValidateToken(status) != nil {
		return nil, fmt.Errorf("invalid status %q", status)
	}
	keys, err := p.view.Keys(ctx)
	if errors.Is(err, jetstream.ErrNoKeysFound) {
		return []Row{}, nil
	}
	if err != nil {
		return nil, err
	}
	digest := sha256.Sum256([]byte(value))
	prefix := "idxa." + key + "." + hex.EncodeToString(digest[:]) + "."
	rows := make([]Row, 0)
	for _, index := range keys {
		if !strings.HasPrefix(index, prefix) {
			continue
		}
		entry, err := p.view.Get(ctx, "row."+strings.TrimPrefix(index, prefix))
		if errors.Is(err, jetstream.ErrKeyNotFound) {
			continue
		}
		if err != nil {
			return nil, err
		}
		var row Row
		if err := json.Unmarshal(entry.Value(), &row); err != nil {
			return nil, err
		}
		actual, exists := row.Attributes[key]
		if exists && actual == value && attributeIndexKey(row, key, value) == index && (status == "" || row.Status == status) {
			rows = append(rows, row)
		}
	}
	sort.Slice(rows, func(i, j int) bool {
		if rows[i].Type == rows[j].Type {
			return rows[i].ID < rows[j].ID
		}
		return rows[i].Type < rows[j].Type
	})
	return rows, nil
}

// Lag reports retained journal messages not yet acknowledged by the durable
// projection consumer, including delivered but unacknowledged messages.
func (p *Projection) Lag(ctx context.Context) (uint64, error) {
	info, err := p.consumer.Info(ctx)
	if err != nil {
		return 0, err
	}
	return info.NumPending + uint64(info.NumAckPending), nil
}

// Run rebuilds once, then consumes journal updates. Periodic rebuilds also
// detect purges, which do not themselves emit journal messages.
func (p *Projection) Run(ctx context.Context) error {
	before, err := p.jrn.Info(ctx)
	if err != nil {
		return err
	}
	rebuiltThrough := before.State.LastSeq
	if err := p.Rebuild(ctx); err != nil {
		return err
	}
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()
	for ctx.Err() == nil {
		select {
		case <-ticker.C:
			if err := p.Rebuild(ctx); err != nil {
				return err
			}
		default:
		}
		batch, err := p.consumer.Fetch(16, jetstream.FetchMaxWait(time.Second))
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			if errors.Is(err, nats.ErrTimeout) || errors.Is(err, jetstream.ErrNoMessages) {
				continue
			}
			return err
		}
		for msg := range batch.Messages() {
			metadata, err := msg.Metadata()
			if err != nil {
				_ = msg.NakWithDelay(time.Second)
				continue
			}
			// This entry committed before Rebuild began. Every retained
			// invocation was read after that watermark, so its row already
			// reflects this entry (or a newer one).
			if metadata.Sequence.Stream <= rebuiltThrough {
				_ = msg.Ack()
				continue
			}
			parts := strings.Split(msg.Subject(), ".")
			if len(parts) != 4 || parts[0] != "wf" || parts[1] != "jrn" {
				_ = msg.Term()
				continue
			}
			if err := p.SyncOne(ctx, parts[2], parts[3]); err != nil {
				_ = msg.NakWithDelay(time.Second)
				continue
			}
			_ = msg.Ack()
		}
		if err := batch.Error(); err != nil && !errors.Is(err, context.DeadlineExceeded) && !errors.Is(err, nats.ErrTimeout) && !errors.Is(err, jetstream.ErrNoMessages) {
			return err
		}
	}
	return nil
}
