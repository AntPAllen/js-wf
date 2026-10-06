//go:build linux

package integration_test

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"
	"unsafe"

	"github.com/nats-io/nats.go/jetstream"
	"js-wf/identity"
	"js-wf/integrity"
	"js-wf/journal"
)

// Account for retained projections, slice growth and output samples. This is a
// conservative application-data charge, not an RSS promise: maps, the SDK,
// decoder temporaries and GC also consume memory. Process limits remain separate.
const matrixBulkMaxChargedBytes = uint64(3 << 30)

type matrixBulkSourceCut struct {
	First, Last, Messages, Bytes uint64
	Consumers                    int
}
type matrixBulkLatencyStats struct {
	SourceCuts              map[string]matrixBulkSourceCut `json:"source_cuts"`
	FinalSourceCuts         map[string]matrixBulkSourceCut `json:"final_source_cuts,omitempty"`
	Records                 map[string]int                 `json:"records"`
	InvocationCutoff        uint64                         `json:"invocation_cutoff"`
	OutOfPrefixChildLookups int                            `json:"out_of_prefix_child_lookups"`
	SnapshotFallbacks       int                            `json:"snapshot_fallbacks"`
	ChargedBytes            uint64                         `json:"charged_bytes"`
}
type matrixBulkInvocation struct {
	typ, id  string
	enabled  time.Time
	records  []journal.Record
	times    []time.Time
	snapshot bool
}
type matrixBulkProjection struct {
	cutoff         uint64
	excluded       map[string]time.Time
	bySubject      map[string]*matrixBulkInvocation
	ordered        []*matrixBulkInvocation
	signals        map[uint64]time.Time
	charged, limit uint64
}

func (p *matrixBulkProjection) charge(bytes uint64) error {
	if p.charged > p.limit || bytes > p.limit-p.charged {
		return fmt.Errorf("bulk latency retained-data budget exceeded")
	}
	p.charged += bytes
	return nil
}

func (p *matrixBulkProjection) invocation(msg *jetstream.RawStreamMsg) error {
	parts := strings.Split(msg.Subject, ".")
	if len(parts) != 4 || parts[0] != "wf" || parts[1] != "inv" || identity.Validate(parts[2], parts[3]) != nil || msg.Time.IsZero() {
		return fmt.Errorf("invalid bulk invocation coordinates")
	}
	subject := identity.JournalSubject(parts[2], parts[3])
	_, excluded := p.excluded[subject]
	if p.bySubject[subject] != nil || excluded {
		return fmt.Errorf("duplicate bulk invocation %s", subject)
	}
	if p.cutoff != 0 && msg.Sequence > p.cutoff {
		if err := p.charge(128 + uint64(2*len(subject))); err != nil {
			return err
		}
		if p.excluded == nil {
			p.excluded = map[string]time.Time{}
		}
		p.excluded[subject] = time.Time{}
		return nil
	}
	if err := p.charge(512 + uint64(2*len(msg.Subject))); err != nil {
		return err
	}
	inv := &matrixBulkInvocation{typ: parts[2], id: parts[3], enabled: msg.Time.UTC()}
	p.bySubject[subject] = inv
	p.ordered = append(p.ordered, inv)
	return nil
}

func (p *matrixBulkProjection) journal(msg *jetstream.RawStreamMsg) error {
	inv := p.bySubject[msg.Subject]
	if _, outside := p.excluded[msg.Subject]; outside && !msg.Time.IsZero() {
		p.excluded[msg.Subject] = msg.Time.UTC()
		return nil
	}
	if inv == nil || msg.Time.IsZero() {
		return fmt.Errorf("bulk journal outside invocation cohort: %s", msg.Subject)
	}
	var entry journal.Entry
	if err := journal.UnmarshalEntry(msg.Data, &entry); err != nil {
		return err
	}
	// Retain only fields consumed by latency reduction. Full protocol/result
	// validation remains the independent complete retained integrity audit.
	var payload []byte
	switch entry.Kind {
	case journal.StepRequested:
		var request struct {
			Kind      string    `json:"kind"`
			FireAt    time.Time `json:"fire_at"`
			ChildType string    `json:"child_type"`
			ChildID   string    `json:"child_id"`
		}
		if err := json.Unmarshal(entry.Payload, &request); err != nil {
			return err
		}
		var err error
		payload, err = json.Marshal(request)
		if err != nil {
			return err
		}
	case journal.SignalConsumed:
		var signal struct {
			Sequence uint64 `json:"sig_seq"`
		}
		if err := json.Unmarshal(entry.Payload, &signal); err != nil {
			return err
		}
		var err error
		payload, err = json.Marshal(signal)
		if err != nil {
			return err
		}
	}
	if err := p.charge(uint64(2*(unsafe.Sizeof(journal.Record{})+unsafe.Sizeof(time.Time{}))) + uint64(len(payload))); err != nil {
		return err
	}
	entry.Payload = payload
	entry.WorkerID = ""
	inv.records = append(inv.records, journal.Record{Entry: entry, Sequence: msg.Sequence})
	inv.times = append(inv.times, msg.Time.UTC())
	return nil
}

// matrixBulkInvocationAudits uses complete raw timestamp cuts plus the shared
// causal evaluator. It requires a separately verified complete integrity report.
// Snapshot invocations retain the existing logical journal/point read path with
// its original 20-second request context. A changed source invalidates everything.
// Server-clock rows continue using their controller receipts, not this adapter.
func matrixBulkInvocationAudits(ctx context.Context, js jetstream.JetStream, expected integrity.Report, completionDeadline time.Time) ([]matrixInvocationAuditResult, matrixBulkLatencyStats, error) {
	return matrixBulkInvocationAuditsThrough(ctx, js, expected, completionDeadline, 0)
}

// Through audits an inclusive invocation prefix while retaining full source
// censuses/stability checks. Out-of-prefix subjects must be proven by WF_INV;
// their last raw journal time is retained for point-equivalent child lookups.
func matrixBulkInvocationAuditsThrough(ctx context.Context, js jetstream.JetStream, expected integrity.Report, completionDeadline time.Time, cutoff uint64) ([]matrixInvocationAuditResult, matrixBulkLatencyStats, error) {
	stats := matrixBulkLatencyStats{SourceCuts: map[string]matrixBulkSourceCut{}, Records: map[string]int{}}
	if _, ok := ctx.Deadline(); !ok || expected.Invocations <= 0 || expected.Journals != expected.Invocations || expected.Terminal != expected.Invocations {
		return nil, stats, fmt.Errorf("bulk latency requires deadline and complete integrity cohort")
	}
	p := matrixBulkProjection{bySubject: map[string]*matrixBulkInvocation{}, signals: map[uint64]time.Time{}, limit: matrixBulkMaxChargedBytes}
	streams := map[string]jetstream.Stream{}
	for _, name := range []string{"WF_INV", "KV_WF_STATE", "WF_JRN", "WF_SIG"} {
		s, err := js.Stream(ctx, name)
		if err != nil {
			return nil, stats, err
		}
		info, err := s.Info(ctx)
		if err != nil {
			return nil, stats, err
		}
		streams[name] = s
		stats.SourceCuts[name] = matrixBulkSourceCut{info.State.FirstSeq, info.State.LastSeq, info.State.Msgs, info.State.Bytes, info.State.Consumers}
	}
	invCut := stats.SourceCuts["WF_INV"]
	var err error
	cutoff, err = matrixBulkCohortCut(invCut, expected.Invocations, cutoff)
	if err != nil {
		return nil, stats, err
	}
	stats.InvocationCutoff, p.cutoff = cutoff, cutoff
	for _, name := range []string{"WF_INV", "KV_WF_STATE", "WF_JRN", "WF_SIG"} {
		err := integrity.WalkRetainedWithChunkedReads(ctx, streams[name], stats.SourceCuts[name].Last, func(msg *jetstream.RawStreamMsg) error {
			stats.Records[name]++
			switch name {
			case "WF_INV":
				return p.invocation(msg)
			case "WF_JRN":
				return p.journal(msg)
			case "KV_WF_STATE":
				if strings.HasPrefix(msg.Subject, "$KV.WF_STATE.snap.") && msg.Header.Get("KV-Operation") != "DEL" && msg.Header.Get("KV-Operation") != "PURGE" {
					parts := strings.Split(strings.TrimPrefix(msg.Subject, "$KV.WF_STATE.snap."), ".")
					if len(parts) != 2 || identity.Validate(parts[0], parts[1]) != nil {
						return fmt.Errorf("invalid snapshot coordinates")
					}
					subject := identity.JournalSubject(parts[0], parts[1])
					if _, outside := p.excluded[subject]; outside {
						return nil
					}
					if p.bySubject[subject] == nil {
						return fmt.Errorf("snapshot outside bulk cohort")
					}
					p.bySubject[subject].snapshot = true
				}
			case "WF_SIG":
				if msg.Time.IsZero() || msg.Sequence == 0 {
					return fmt.Errorf("invalid bulk signal coordinates")
				}
				if _, ok := p.signals[msg.Sequence]; ok {
					return fmt.Errorf("duplicate bulk signal sequence")
				}
				if err := p.charge(128); err != nil {
					return err
				}
				p.signals[msg.Sequence] = msg.Time.UTC()
			}
			return nil
		})
		if err != nil {
			stats.ChargedBytes = p.charged
			return nil, stats, err
		}
		if uint64(stats.Records[name]) != stats.SourceCuts[name].Messages {
			return nil, stats, fmt.Errorf("incomplete bulk stream census %s", name)
		}
	}
	if len(p.ordered) != expected.Invocations {
		return nil, stats, fmt.Errorf("incomplete bulk invocation census")
	}
	metadata := &matrixLatencyMetadataJS{JetStream: js}
	results := make([]matrixInvocationAuditResult, 0, len(p.ordered))
	logicalEntries := 0
	for _, inv := range p.ordered {
		if err := ctx.Err(); err != nil {
			return nil, stats, err
		}
		var samples []matrixLatencySample
		var err error
		if inv.snapshot {
			stats.SnapshotFallbacks++
			attempt, stop := context.WithTimeout(ctx, 20*time.Second)
			var records []journal.Record
			records, _, err = journal.New(metadata).Read(attempt, inv.typ, inv.id)
			if err == nil {
				logicalEntries += len(records)
				samples, err = matrixInvocationLatencies(attempt, metadata, inv.typ, inv.id, inv.enabled, completionDeadline)
			}
			stop()
		} else {
			logicalEntries += len(inv.records)
			if len(inv.records) == 0 || inv.records[0].Kind != journal.Started {
				return nil, stats, fmt.Errorf("bulk journal missing retained prefix")
			}
			for index, record := range inv.records {
				if record.Index != uint64(index) {
					return nil, stats, fmt.Errorf("bulk journal index gap")
				}
			}
			last := inv.records[len(inv.records)-1]
			if last.Kind != journal.Completed && last.Kind != journal.Failed {
				return nil, stats, fmt.Errorf("bulk journal missing terminal")
			}
			samples, err = matrixReduceInvocationLatencies(ctx, inv.typ, inv.id, inv.enabled, completionDeadline, 0, inv.records, inv.times,
				func(typ, id string) (time.Time, error) {
					child := p.bySubject[identity.JournalSubject(typ, id)]
					if child == nil {
						if at := p.excluded[identity.JournalSubject(typ, id)]; !at.IsZero() {
							stats.OutOfPrefixChildLookups++
							return at, nil
						}
						return time.Time{}, jetstream.ErrMsgNotFound
					}
					if len(child.times) == 0 {
						return time.Time{}, jetstream.ErrMsgNotFound
					}
					return child.times[len(child.times)-1], nil
				},
				func(seq uint64) (time.Time, error) {
					at, ok := p.signals[seq]
					if !ok {
						return time.Time{}, jetstream.ErrMsgNotFound
					}
					return at, nil
				},
				func() (time.Time, error) { return inv.times[len(inv.times)-1], nil })
		}
		if err != nil {
			return nil, stats, err
		}
		if err := p.charge(uint64(2*len(samples)) * uint64(unsafe.Sizeof(matrixLatencySample{}))); err != nil {
			return nil, stats, err
		}
		results = append(results, matrixInvocationAuditResult{samples: samples})
	}
	stats.ChargedBytes = p.charged
	if logicalEntries != expected.Entries {
		return nil, stats, fmt.Errorf("bulk logical journal entry census differs from full integrity audit")
	}
	stats.FinalSourceCuts = map[string]matrixBulkSourceCut{}
	for name, s := range streams {
		info, err := s.Info(ctx)
		if err != nil {
			return nil, stats, err
		}
		stats.FinalSourceCuts[name] = matrixBulkSourceCut{info.State.FirstSeq, info.State.LastSeq, info.State.Msgs, info.State.Bytes, info.State.Consumers}
	}
	for _, name := range []string{"WF_INV", "KV_WF_STATE", "WF_JRN", "WF_SIG"} {
		if stats.FinalSourceCuts[name] != stats.SourceCuts[name] {
			return nil, stats, fmt.Errorf("bulk source changed or audit cursor remains: %s before=%+v after=%+v", name, stats.SourceCuts[name], stats.FinalSourceCuts[name])
		}
	}
	if err := ctx.Err(); err != nil {
		return nil, stats, err
	}
	return results, stats, nil
}

func matrixBulkCohortCut(cut matrixBulkSourceCut, count int, cutoff uint64) (uint64, error) {
	if count <= 0 || cut.First == 0 || cut.Last < cut.First || cut.Last-cut.First+1 != cut.Messages {
		return 0, fmt.Errorf("bulk invocation source count/hole differs from point audit")
	}
	if cutoff == 0 {
		cutoff = cut.Last
	}
	if cutoff < cut.First || cutoff > cut.Last || cutoff-cut.First+1 != uint64(count) {
		return 0, fmt.Errorf("bulk invocation prefix/count differs from point audit")
	}
	return cutoff, nil
}
