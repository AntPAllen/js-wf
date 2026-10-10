package graphpublication

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

// An acknowledged absent-scope registration is a stream sequence barrier.
// It is never a grant. Reaffirming it makes it the last owner marker at the
// boundary; it must be discovered before the scan can claim completeness.
func ownerScanBarrierKey(owner string) string {
	// Keep absence witnesses scoped to the owner; a global reserved blob key
	// would make unrelated concurrent renewals compete on one physical subject.
	return key([]byte("graph-owner-scan-barrier/" + owner))
}

var _ OwnerScopeScanPort = (*OwnerIndexedNativePort)(nil)

type nativeOwnerScopeScan struct {
	port                           *OwnerIndexedNativePort
	owner, prefix                  string
	barrier, next, expected, found uint64
	done                           bool
	err                            error
}

func (p *OwnerIndexedNativePort) BeginOwnerScopeScan(ctx context.Context, owner string) (OwnerScopeScan, error) {
	barrier, err := p.ownerScopeWitnessSequence(ctx, owner, ownerScanBarrierKey(owner), true)
	if err != nil {
		return nil, err
	}
	count, err := p.ownerScopeCount(ctx, owner, barrier)
	if err != nil {
		return nil, err
	}
	return &nativeOwnerScopeScan{port: p, owner: owner, prefix: p.prefix + ".owner." + key([]byte(owner)) + ".", barrier: barrier, next: 1, expected: count}, nil
}

// STREAM.INFO's documented filtered pagination response includes total.
// Requesting an offset beyond the supported owner cardinality returns only
// the count, avoiding a complete key payload. NumSubjects is stream-wide.
// The acknowledged barrier precedes this leader response; lag is rejected.
// API reference: https://docs.nats.io/reference/reference-protocols/nats_api_reference
func (p *OwnerIndexedNativePort) ownerScopeCount(ctx context.Context, owner string, barrier uint64) (uint64, error) {
	const beyond = 2147483647
	request := struct {
		Filter string `json:"subjects_filter"`
		Offset int    `json:"offset"`
	}{p.prefix + ".owner." + key([]byte(owner)) + ".>", beyond}
	data, err := json.Marshal(request)
	if err != nil {
		return 0, err
	}
	opts := p.js.Options()
	if _, bounded := ctx.Deadline(); !bounded {
		timeout := opts.DefaultTimeout
		if timeout <= 0 {
			timeout = 5 * time.Second
		}
		var stop context.CancelFunc
		ctx, stop = context.WithTimeout(ctx, timeout)
		defer stop()
	}
	prefix := strings.TrimSuffix(opts.APIPrefix, ".")
	if opts.Domain != "" {
		prefix = "$JS." + opts.Domain + ".API"
	} else if prefix == "" {
		prefix = "$JS.API"
	}
	subject := prefix + ".STREAM.INFO." + p.name
	if opts.ClientTrace != nil && opts.ClientTrace.RequestSent != nil {
		opts.ClientTrace.RequestSent(subject, data)
	}
	msg, err := p.js.Conn().RequestWithContext(ctx, subject, data)
	if err != nil {
		return 0, err
	}
	if opts.ClientTrace != nil && opts.ClientTrace.ResponseReceived != nil {
		opts.ClientTrace.ResponseReceived(subject, msg.Data, msg.Header)
	}
	if len(msg.Data) == 0 || len(msg.Data) > 65536 {
		return 0, errors.New("invalid owner count response size")
	}
	var response struct {
		Type   string              `json:"type"`
		Error  *jetstream.APIError `json:"error"`
		Total  uint64              `json:"total"`
		Config struct {
			Name string `json:"name"`
		} `json:"config"`
		State struct {
			Last     uint64            `json:"last_seq"`
			Subjects map[string]uint64 `json:"subjects"`
		} `json:"state"`
	}
	if err = json.Unmarshal(msg.Data, &response); err != nil {
		return 0, err
	}
	if response.Error != nil {
		return 0, response.Error
	}
	if response.Type != "io.nats.jetstream.api.v1.stream_info_response" || response.Config.Name != p.name || response.Total == 0 || response.Total > beyond || response.State.Last < barrier || len(response.State.Subjects) != 0 {
		return 0, errors.New("invalid or lagging owner count response")
	}
	return response.Total, nil
}

func (s *nativeOwnerScopeScan) Advance(ctx context.Context, budget uint64) (keys []string, done bool, err error) {
	if s.err != nil {
		return nil, false, s.err
	}
	if err = ctx.Err(); err != nil {
		return nil, false, err
	}
	if budget == 0 {
		return nil, false, errors.New("positive owner scan budget required")
	}
	if s.done {
		return nil, true, nil
	}
	defer func() {
		if err != nil {
			s.err = err
			keys = nil
			done = false
		}
	}()
	for uint64(len(keys)) < budget {
		msg, e := s.port.stream.GetMsg(ctx, s.next, jetstream.WithGetMsgSubject(s.prefix+">"))
		if e != nil {
			if errors.Is(e, jetstream.ErrMsgNotFound) {
				return nil, false, errors.New("incomplete owner scope scan")
			}
			return nil, false, e
		}
		k := strings.TrimPrefix(msg.Subject, s.prefix)
		if !strings.HasPrefix(msg.Subject, s.prefix) || !validHash(k) || msg.Sequence < s.next || msg.Sequence > s.barrier || msg.Sequence == ^uint64(0) {
			return nil, false, errors.New("invalid or incomplete owner scope scan")
		}
		want, e := json.Marshal(ownerScopeValue{ownerIndexSchema, s.owner, k})
		if e != nil || !bytes.Equal(msg.Data, want) {
			return nil, false, errors.New("invalid owner scan marker")
		}
		s.found++
		if s.found > s.expected {
			return nil, false, errors.New("owner scan count exceeded")
		}
		keys = append(keys, k)
		s.next = msg.Sequence + 1
		if msg.Sequence == s.barrier {
			if k != ownerScanBarrierKey(s.owner) || s.found != s.expected {
				return nil, false, fmt.Errorf("incomplete owner scope scan: found %d expected %d", s.found, s.expected)
			}
			s.done = true
			return keys, true, nil
		}
	}
	return keys, false, nil
}
