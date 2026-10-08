package retainedindex

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"math/rand"
	"testing"
)

type memory struct {
	packets [][]byte
	reads   int
	failure error
}

func (m *memory) ReadIndex(ctx context.Context, i uint64) ([]byte, error) {
	m.reads++
	if m.failure != nil {
		return nil, m.failure
	}
	if i >= uint64(len(m.packets)) {
		return nil, ErrInvalid
	}
	return bytes.Clone(m.packets[i]), ctx.Err()
}
func put(t *testing.T, m *memory, key Key, value uint64) {
	t.Helper()
	packet, _, _, err := Update(context.Background(), m, uint64(len(m.packets)), key, value)
	if err != nil {
		t.Fatal(err)
	}
	if packet != nil {
		if len(packet) > MaxPacketBytes {
			t.Fatal("unbounded packet", len(packet))
		}
		m.packets = append(m.packets, bytes.Clone(packet))
	}
}
func TestPersistentIndexMatchesMapAndCapturedPopulations(t *testing.T) {
	for seed := int64(1); seed <= 32; seed++ {
		t.Run(fmt.Sprintf("seed%d", seed), func(t *testing.T) {
			m := &memory{}
			random := rand.New(rand.NewSource(seed))
			expected := map[Key]uint64{}
			type snapshot struct {
				count  uint64
				values map[Key]uint64
			}
			snapshots := []snapshot{}
			for i := 0; i < 512; i++ {
				key := Key(sha256.Sum256([]byte(fmt.Sprintf("key%d", random.Intn(256)))))
				value := random.Uint64()
				old, found := expected[key]
				packet, got, exists, err := Update(context.Background(), m, uint64(len(m.packets)), key, value)
				if err != nil || found != exists || found && old != got || packet == nil {
					t.Fatal("map update differs", i, got, exists, err)
				}
				m.packets = append(m.packets, bytes.Clone(packet))
				expected[key] = value
				if i%64 == 0 {
					copy := map[Key]uint64{}
					for k, v := range expected {
						copy[k] = v
					}
					snapshots = append(snapshots, snapshot{uint64(len(m.packets)), copy})
				}
			}
			snapshots = append(snapshots, snapshot{uint64(len(m.packets)), expected})
			for _, snap := range snapshots {
				for i := 0; i < 300; i++ {
					key := Key(sha256.Sum256([]byte(fmt.Sprintf("key%d", i))))
					want, found := snap.values[key]
					m.reads = 0
					got, exists, err := Lookup(context.Background(), m, snap.count, key)
					if err != nil || exists != found || found && got != want || m.reads > MaxNodes {
						t.Fatal("snapshot lookup differs", snap.count, i, got, exists, m.reads, err)
					}
				}
			}
			// Repeating an identical indexed operation must not grow storage.
			for key, value := range expected {
				p, old, found, err := Update(context.Background(), m, uint64(len(m.packets)), key, value)
				if err != nil || p != nil || old != value || !found {
					t.Fatal("identical update grew index", err)
				}
			}
		})
	}
}

func TestIndexMaximumDepthAndPacketBound(t *testing.T) {
	m := &memory{}
	key := Key{}
	put(t, m, key, 0)
	for i := uint16(0); i < 256; i++ {
		k := Key{}
		k[i/8] = 0x80 >> uint(i%8)
		put(t, m, k, uint64(i)+1)
	}
	m.reads = 0
	value, found, err := Lookup(context.Background(), m, uint64(len(m.packets)), key)
	if err != nil || !found || value != 0 || m.reads > MaxNodes {
		t.Fatal(value, found, m.reads, err)
	}
	p, old, found, err := Update(context.Background(), m, uint64(len(m.packets)), key, math.MaxUint64)
	if err != nil || !found || old != 0 || len(p) != MaxPacketBytes || int(binary.BigEndian.Uint16(p[8:])) != MaxNodes {
		t.Fatal("maximum copied path bound differs", len(p), old, found, err)
	}
	m.packets = append(m.packets, p)
	for i := uint16(0); i < 256; i++ {
		k := Key{}
		k[i/8] = 0x80 >> uint(i%8)
		v, ok, e := Lookup(context.Background(), m, uint64(len(m.packets)), k)
		if e != nil || !ok || v != uint64(i)+1 {
			t.Fatal("deep update lost sibling", i, v, ok, e)
		}
	}
}

func TestIndexCorruptionUncertaintyAndCancellation(t *testing.T) {
	m := &memory{}
	a, b := Key{}, Key{}
	b[0] = 128
	put(t, m, a, 1)
	put(t, m, b, 2)
	original := bytes.Clone(m.packets[1])
	cases := map[string]func([]byte) []byte{
		"schema":         func(p []byte) []byte { p[0] ^= 1; return p },
		"trailing":       func(p []byte) []byte { return append(p, 0) },
		"header_padding": func(p []byte) []byte { p[10] = 1; return p },
		"node_padding":   func(p []byte) []byte { p[headerBytes+34] = 1; return p },
		"leaf_children":  func(p []byte) []byte { p[headerBytes+48] = 1; return p },
		"branch_value":   func(p []byte) []byte { p[headerBytes+nodeBytes+47] = 1; return p },
		"branch_prefix":  func(p []byte) []byte { p[headerBytes+nodeBytes] = 1; return p },
		"cycle":          func(p []byte) []byte { writeLocation(p[headerBytes+nodeBytes+48:], location{1, 1}); return p },
		"future":         func(p []byte) []byte { writeLocation(p[headerBytes+nodeBytes+48:], location{2, 0}); return p },
		"sibling_alias": func(p []byte) []byte {
			copy(p[headerBytes+nodeBytes+48:headerBytes+nodeBytes+64], p[headerBytes+nodeBytes+64:])
			return p
		},
		"slot_outside_old_packet": func(p []byte) []byte { writeLocation(p[headerBytes+nodeBytes+48:], location{0, 12}); return p },
		"disconnected_new_node":   func(p []byte) []byte { writeLocation(p[headerBytes+nodeBytes+64:], location{0, 0}); return p },
	}
	for name, corrupt := range cases {
		t.Run(name, func(t *testing.T) {
			m.packets[1] = corrupt(bytes.Clone(original))
			_, _, err := Lookup(context.Background(), m, 2, a)
			if !errors.Is(err, ErrInvalid) {
				t.Fatal("corruption became absence", err)
			}
		})
	}
	m.packets[1] = original
	m.failure = context.DeadlineExceeded
	if _, _, err := Lookup(context.Background(), m, 2, a); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal(err)
	}
	if _, _, _, err := Update(context.Background(), m, 2, a, 2); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	m.failure = nil
	if _, _, err := Lookup(ctx, m, 0, a); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if _, _, _, err := Update(ctx, m, 0, a, 1); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if _, _, err := Lookup(context.Background(), m, math.MaxUint64, a); !errors.Is(err, ErrInvalid) {
		t.Fatal(err)
	}
	if _, _, _, err := Update(context.Background(), m, math.MaxInt64, a, 1); !errors.Is(err, ErrInvalid) {
		t.Fatal(err)
	}
}
