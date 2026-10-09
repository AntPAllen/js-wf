package worker

import (
	"bytes"
	"context"
	"errors"
	"fmt"

	"github.com/nats-io/nats.go/jetstream"
	"js-wf/client"
	"js-wf/identity"
	"js-wf/retention"
)

// repairVerifiedGraphOutcome is called only after owned canonical terminal
// validation. Mirror bytes never authorize delivery or select an outcome.
// A single conditional mutation protects a concurrent purge/projection write.
func repairVerifiedGraphOutcome(ctx context.Context, port OutcomePort, typ, id string, invocation uint64, payload []byte) error {
	key := identity.Key(typ, id)
	previous, err := port.Get(ctx, key)
	if errors.Is(err, jetstream.ErrKeyNotFound) {
		_, err = port.Create(ctx, key, payload)
	} else if err == nil {
		marker, tomb, decodeErr := retention.Decode(previous.Value)
		if decodeErr != nil {
			return decodeErr
		}
		if tomb && marker.InvSeq >= invocation {
			return client.ErrPurged
		}
		if bytes.Equal(previous.Value, payload) {
			return nil
		}
		if previous.Revision == 0 {
			return fmt.Errorf("graph projection has no revision")
		}
		_, err = port.Update(ctx, key, payload, previous.Revision)
	} else {
		return err
	}
	if err == nil {
		return nil
	}
	// A committed lost acknowledgment can be witnessed, but a conflicting or
	// uncertain read must not authorize another write or dispatch ACK.
	current, readErr := port.Get(ctx, key)
	if readErr == nil {
		marker, tomb, decodeErr := retention.Decode(current.Value)
		if decodeErr != nil {
			return decodeErr
		}
		if tomb && marker.InvSeq >= invocation {
			return client.ErrPurged
		}
		if current.Revision > previous.Revision && bytes.Equal(current.Value, payload) {
			return nil
		}
	}
	return err
}
