package assignment

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	"js-wf/lease"
	"js-wf/provision"
)

const MembershipBucket = "WF_MEMBERS"
const MembershipHeartbeat = 3 * time.Second

// Membership uses server-expiring KV registrations with revision-fenced leases.
// It owns a separate bucket so listing membership does not scan workflow leases.
type Membership struct {
	kv     jetstream.KeyValue
	leases *lease.Store
}

// EnsureMembership creates or verifies the optional automatic-assignment bucket.
// An existing bucket's expiry/replication settings are never silently changed.
func EnsureMembership(ctx context.Context, js jetstream.JetStream, replicas int) (*Membership, error) {
	if replicas < 1 || replicas > 5 {
		return nil, fmt.Errorf("membership replicas must be 1..5")
	}
	kv, err := js.KeyValue(ctx, MembershipBucket)
	if errors.Is(err, jetstream.ErrBucketNotFound) {
		kv, err = js.CreateKeyValue(ctx, jetstream.KeyValueConfig{Bucket: MembershipBucket, History: 1, TTL: provision.LeaseTTL, LimitMarkerTTL: time.Minute, Storage: jetstream.FileStorage, Replicas: replicas})
		if err != nil {
			kv, err = js.KeyValue(ctx, MembershipBucket)
		}
	}
	if err != nil {
		return nil, err
	}
	status, err := kv.Status(ctx)
	if err != nil {
		return nil, err
	}
	cfg := status.Config()
	if status.History() != 1 || status.TTL() != provision.LeaseTTL || status.LimitMarkerTTL() != time.Minute || cfg.Storage != jetstream.FileStorage || cfg.Replicas != replicas || cfg.MaxBytes > 0 {
		return nil, fmt.Errorf("bucket %s configuration mismatch", MembershipBucket)
	}
	return &Membership{kv: kv, leases: lease.NewWithKeyValue(kv)}, nil
}

func membershipID(id string) string {
	sum := sha256.Sum256([]byte(id))
	return hex.EncodeToString(sum[:])
}

// Register rejects a duplicate live worker ID. The returned lease must be
// renewed; loss of its revision fences an expired process after ID reuse.
func (m *Membership) Register(ctx context.Context, id string) (*lease.Lease, error) {
	if err := validOwner(id); err != nil {
		return nil, err
	}
	return m.leases.Acquire(ctx, "member", membershipID(id), id)
}

// Live returns a sorted snapshot of initialized registrations. Server KV expiry
// determines liveness; worker wall clocks are never used to expire another owner.
func (m *Membership) Live(ctx context.Context) ([]string, error) {
	keys, err := m.kv.Keys(ctx)
	if errors.Is(err, jetstream.ErrNoKeysFound) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var members []string
	for _, key := range keys {
		if !strings.HasPrefix(key, "member.") {
			continue
		}
		entry, err := m.kv.Get(ctx, key)
		if errors.Is(err, jetstream.ErrKeyNotFound) {
			continue
		}
		if err != nil {
			return nil, err
		}
		var value lease.Value
		if err := json.Unmarshal(entry.Value(), &value); err != nil {
			return nil, err
		}
		if validOwner(value.Worker) != nil || key != "member."+membershipID(value.Worker) {
			return nil, fmt.Errorf("invalid membership entry %s", key)
		}
		if value.Epoch == 0 {
			continue
		}
		members = append(members, value.Worker)
	}
	sort.Strings(members)
	return members, nil
}

type coordinatedAssignments struct {
	RebalancePort
	leader *lease.Lease
}

func (p coordinatedAssignments) Assign(ctx context.Context, partition uint32, owner string, revision uint64) (uint64, error) {
	if err := p.leader.Renew(ctx); err != nil {
		return 0, err
	}
	return p.RebalancePort.Assign(ctx, partition, owner, revision)
}

// Controller owns one process registration and its optional coordinator lease.
// Construct it before starting partition loops; Run must be joined before Close.
type Controller struct {
	membership   *Membership
	id           string
	assignments  RebalancePort
	registration *lease.Lease
	coordinator  *lease.Lease
}

func (m *Membership) Controller(ctx context.Context, id string, assignments RebalancePort) (*Controller, error) {
	if assignments == nil {
		return nil, fmt.Errorf("nil assignment port")
	}
	registration, err := m.Register(ctx, id)
	if err != nil {
		return nil, err
	}
	return &Controller{membership: m, id: id, assignments: assignments, registration: registration}, nil
}

// Close releases only this process's retained revisions. Server expiry is the
// backstop if cleanup cannot reach the cluster. Join Run before calling Close.
func (c *Controller) Close() {
	cleanup, done := context.WithTimeout(context.Background(), 2*time.Second)
	defer done()
	if c.coordinator != nil {
		_ = c.coordinator.Release(cleanup)
	}
	_ = c.registration.Release(cleanup)
}

// Run drives automatic assignments under a coordinator lease. A registration
// or transport failure returns to the caller, which must stop worker loops.
// All members must register the same handler set. RunKVAssignments consumes the
// resulting ownership map. Membership liveness uses server expiry, not clocks.
func (c *Controller) Run(ctx context.Context) error {
	m, id, assignments, registration := c.membership, c.id, c.assignments, c.registration
	ticker := time.NewTicker(MembershipHeartbeat)
	defer ticker.Stop()
	for {
		if ctx.Err() != nil {
			return nil
		}
		// Bound a pass below the membership TTL; a stopped/isolated process must
		// fail closed instead of letting a stale membership drive new assignments.
		attempt, done := context.WithTimeout(ctx, 8*time.Second)
		err := registration.Renew(attempt)
		if err == nil {
			if c.coordinator == nil {
				c.coordinator, err = m.leases.Acquire(attempt, "coordinator", "assign", id)
				if errors.Is(err, lease.ErrHeld) {
					err = nil
				}
			} else {
				err = c.coordinator.Renew(attempt)
			}
		}
		if err == nil && c.coordinator != nil {
			var members []string
			members, err = m.Live(attempt)
			if err == nil {
				if len(members) == 0 {
					err = fmt.Errorf("live membership disappeared")
				} else {
					_, err = Rebalance(attempt, coordinatedAssignments{RebalancePort: assignments, leader: c.coordinator}, members, false)
				}
			}
		}
		done()
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return err
		}
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
		}
	}
}
