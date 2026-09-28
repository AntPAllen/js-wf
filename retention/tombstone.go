package retention

import (
	"encoding/json"
	"fmt"
	"time"
)

// Tombstone marks a retired invocation generation after its signals and
// journal have been purged. InvSeq prevents a reused ID seeing old state.
type Tombstone struct {
	Tombstone bool      `json:"tombstone"`
	InvSeq    uint64    `json:"inv_seq"`
	PurgedAt  time.Time `json:"purged_at"`
	ExpiresAt time.Time `json:"expires_at"`
}

func Decode(value []byte) (Tombstone, bool, error) {
	var marker struct {
		Tombstone bool `json:"tombstone"`
	}
	if err := json.Unmarshal(value, &marker); err != nil {
		return Tombstone{}, false, err
	}
	if !marker.Tombstone {
		return Tombstone{}, false, nil
	}
	var tomb Tombstone
	if err := json.Unmarshal(value, &tomb); err != nil {
		return Tombstone{}, false, err
	}
	if tomb.InvSeq == 0 || tomb.PurgedAt.IsZero() || tomb.ExpiresAt.Before(tomb.PurgedAt) {
		return Tombstone{}, false, fmt.Errorf("invalid invocation tombstone")
	}
	return tomb, true, nil
}
