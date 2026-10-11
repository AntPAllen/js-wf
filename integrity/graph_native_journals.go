package integrity

import (
	"context"
	"fmt"
	"strings"

	"github.com/nats-io/nats.go/jetstream"
)

// CheckNativeGraphJournals audits one isolated runtime namespace, including all
// retained WF_INV sources and current terminal WF_STATE values. All invocations
// must use that namespace; mixing legacy/other graph sources is an error. The
// caller must enforce the same full quiescence contract as the reference audit.
func CheckNativeGraphJournals(ctx context.Context, js jetstream.JetStream, ns GraphAuditNamespace) (report GraphJournalReport, err error) {
	if js == nil {
		return report, fmt.Errorf("nil native graph runtime audit transport")
	}
	invocations, err := js.Stream(ctx, "WF_INV")
	if err != nil {
		return report, err
	}
	before, err := invocations.Info(ctx)
	if err != nil {
		return report, err
	}
	sources := map[string]*jetstream.RawStreamMsg{}
	err = scan(ctx, invocations, func(source *jetstream.RawStreamMsg) error {
		if !strings.HasPrefix(source.Subject, "wf.inv.") {
			return fmt.Errorf("foreign invocation subject")
		}
		key := strings.TrimPrefix(source.Subject, "wf.inv.")
		if sources[key] != nil || source.Sequence > before.State.LastSeq {
			return fmt.Errorf("duplicate/moving canonical invocation source")
		}
		sources[key] = source
		return nil
	})
	if err != nil {
		return report, err
	}
	if uint64(len(sources)) != before.State.Msgs {
		return report, fmt.Errorf("canonical invocation census differs")
	}
	state, err := js.KeyValue(ctx, "WF_STATE")
	if err != nil {
		return report, err
	}
	stateStream, err := js.Stream(ctx, "KV_WF_STATE")
	if err != nil {
		return report, err
	}
	stateBefore, err := stateStream.Info(ctx)
	if err != nil {
		return report, err
	}
	_, err = checkNativeGraphSnapshot(ctx, js, ns, func(call context.Context, graph GraphReferenceSnapshot) (GraphReferenceReport, error) {
		var checkErr error
		report, checkErr = CheckGraphJournals(call, GraphJournalSnapshot{Graph: graph, Invocations: sources, ReadProjection: func(read context.Context, key string) ([]byte, error) {
			value, err := state.Get(read, key)
			if err != nil {
				return nil, err
			}
			return value.Value(), nil
		}})
		return report.References, checkErr
	})
	if err != nil {
		return report, err
	}
	after, err := invocations.Info(ctx)
	if err != nil {
		return report, err
	}
	if before.State.LastSeq != after.State.LastSeq || before.State.FirstSeq != after.State.FirstSeq || before.State.Msgs != after.State.Msgs {
		return report, fmt.Errorf("canonical invocations changed during audit")
	}
	stateAfter, err := stateStream.Info(ctx)
	if err != nil {
		return report, err
	}
	if stateBefore.State.LastSeq != stateAfter.State.LastSeq || stateBefore.State.FirstSeq != stateAfter.State.FirstSeq || stateBefore.State.Msgs != stateAfter.State.Msgs {
		return report, fmt.Errorf("canonical projections changed during audit")
	}
	return report, nil
}
