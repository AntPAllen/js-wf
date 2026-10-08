package retention

import (
	"context"
	"errors"
	"fmt"
	"github.com/nats-io/nats.go/jetstream"
	"js-wf/identity"
	"sort"
	"strconv"
	"strings"
)

// NativeTimerRetirePort exposes retained hints, not delivery or clock decisions.
// A terminal journal is required before RetireNativeTimerHints is called.
type NativeTimerRetirePort interface {
	NativeTimerSubjects(context.Context, string, string) ([]string, error)
	LastNativeTimer(context.Context, string) (*jetstream.RawStreamMsg, error)
	DeleteNativeTimer(context.Context, uint64) error
}

// RetireNativeTimerHints removes only observed messages for the terminal
// invocation generation. Sequence deletion cannot erase a concurrent replacement
// on the same subject; later publications are found on the next scan. Lost delete
// replies are ambiguous and retries are safe. Pending timers never enter here.
func RetireNativeTimerHints(ctx context.Context, port NativeTimerRetirePort, typ, id string, generation uint64, dryRun bool) (int, error) {
	if err := identity.Validate(typ, id); err != nil {
		return 0, err
	}
	if generation == 0 {
		return 0, fmt.Errorf("invalid terminal generation")
	}
	subjects, err := port.NativeTimerSubjects(ctx, typ, id)
	if err != nil {
		return 0, err
	}
	sort.Strings(subjects)
	var removed int
	prefix := "wf.schedule." + typ + "." + id + "."
	for _, subject := range subjects {
		if !strings.HasPrefix(subject, prefix) {
			return removed, fmt.Errorf("native schedule subject outside terminal invocation: %q", subject)
		}
		step, err := strconv.ParseUint(strings.TrimPrefix(subject, prefix), 10, 64)
		if err != nil {
			return removed, fmt.Errorf("invalid native schedule step: %q", subject)
		}
		hint, err := port.LastNativeTimer(ctx, subject)
		if errors.Is(err, jetstream.ErrMsgNotFound) {
			continue
		}
		if err != nil {
			return removed, err
		}
		if hint == nil || hint.Subject != subject || hint.Sequence == 0 || string(hint.Data) != identity.Key(typ, id) {
			return removed, fmt.Errorf("invalid retained native schedule: %q", subject)
		}
		invSeq, err := strconv.ParseUint(hint.Header.Get(identity.TimerInvSeqHeader), 10, 64)
		if err != nil || invSeq == 0 || hint.Header.Get(identity.TimerStepHeader) != strconv.FormatUint(step, 10) {
			return removed, fmt.Errorf("invalid native schedule generation/step: %q", subject)
		}
		if invSeq != generation {
			continue
		}
		if !dryRun {
			if err := port.DeleteNativeTimer(ctx, hint.Sequence); err != nil && !errors.Is(err, jetstream.ErrMsgNotFound) {
				return removed, err
			}
		}
		removed++
	}
	return removed, nil
}
