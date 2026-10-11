package integrity

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"testing"

	"js-wf/internal/retainedindex"
)

func rawIndexPacket(nodes ...auditedIndexNode) []byte {
	data := make([]byte, 16+80*len(nodes))
	copy(data, "JWFIDX01")
	binary.BigEndian.PutUint16(data[8:10], uint16(len(nodes)))
	for i, n := range nodes {
		b := data[16+i*80 : 16+(i+1)*80]
		copy(b[:32], n.key[:])
		binary.BigEndian.PutUint16(b[32:34], n.bit)
		binary.BigEndian.PutUint64(b[40:48], n.value)
		binary.BigEndian.PutUint64(b[48:56], n.left.record)
		binary.BigEndian.PutUint16(b[56:58], n.left.slot)
		binary.BigEndian.PutUint64(b[64:72], n.right.record)
		binary.BigEndian.PutUint16(b[72:74], n.right.slot)
	}
	return data
}

func TestRawGraphSignalIndexIndependentPrefixes(t *testing.T) {
	keys := [][32]byte{{}, {0x80}, {0x40}}
	leaf := func(i int) auditedIndexNode { return auditedIndexNode{key: keys[i], bit: 256, value: uint64(i)} }
	packets := [][]byte{
		rawIndexPacket(leaf(0)),
		rawIndexPacket(leaf(1), auditedIndexNode{bit: 0, left: auditedIndexLocation{0, 0}, right: auditedIndexLocation{1, 0}}),
		rawIndexPacket(leaf(2), auditedIndexNode{bit: 1, left: auditedIndexLocation{0, 0}, right: auditedIndexLocation{2, 0}}, auditedIndexNode{bit: 0, left: auditedIndexLocation{2, 1}, right: auditedIndexLocation{1, 0}}),
	}
	t.Run("valid", func(t *testing.T) {
		var s auditedSignalIndex
		for i, p := range packets {
			if err := s.append(context.Background(), p, keys[i], uint64(i)); err != nil {
				t.Fatal(i, err)
			}
		}
	})
	controls := map[string]func([]byte) []byte{
		"magic":               func(p []byte) []byte { p[0]++; return p },
		"header-reserved":     func(p []byte) []byte { p[10]++; return p },
		"truncated":           func(p []byte) []byte { return p[:len(p)-1] },
		"count":               func(p []byte) []byte { p[9]++; return p },
		"leaf-key":            func(p []byte) []byte { p[16]++; return p },
		"leaf-value":          func(p []byte) []byte { p[63]++; return p },
		"leaf-child":          func(p []byte) []byte { p[71]++; return p },
		"bit-overflow":        func(p []byte) []byte { p[49]++; return p },
		"node-reserved":       func(p []byte) []byte { p[50]++; return p },
		"left-reserved":       func(p []byte) []byte { p[74]++; return p },
		"right-reserved":      func(p []byte) []byte { p[90]++; return p },
		"branch-value":        func(p []byte) []byte { p[143]++; return p },
		"branch-prefix":       func(p []byte) []byte { p[96]++; return p },
		"branch-bit-order":    func(p []byte) []byte { p[129] = 0; return p },
		"future-record":       func(p []byte) []byte { p[151] = 3; return p },
		"same-record-forward": func(p []byte) []byte { p[151] = 2; p[153] = 2; return p },
		"old-slot-missing":    func(p []byte) []byte { p[153] = 9; return p },
		"wrong-side":          func(p []byte) []byte { p[151] = 1; return p },
		"lost-prefix":         func(p []byte) []byte { return rawIndexPacket(leaf(2)) },
		"lost-one-old-key": func(p []byte) []byte {
			return rawIndexPacket(leaf(2), auditedIndexNode{bit: 0, left: auditedIndexLocation{2, 0}, right: auditedIndexLocation{1, 0}})
		},
		"unreachable-node": func(p []byte) []byte {
			return rawIndexPacket(leaf(2), auditedIndexNode{bit: 1, left: auditedIndexLocation{0, 0}, right: auditedIndexLocation{2, 0}}, auditedIndexNode{bit: 0, left: auditedIndexLocation{2, 0}, right: auditedIndexLocation{1, 0}})
		},
	}
	for name, alter := range controls {
		t.Run(name, func(t *testing.T) {
			var s auditedSignalIndex
			for i := 0; i < 2; i++ {
				if err := s.append(context.Background(), packets[i], keys[i], uint64(i)); err != nil {
					t.Fatal(err)
				}
			}
			if err := s.append(context.Background(), alter(append([]byte(nil), packets[2]...)), keys[2], 2); err == nil {
				t.Fatal("corrupt persistent prefix accepted")
			}
			if len(s.packets) != 2 {
				t.Fatal("failed packet changed admitted prefix")
			}
		})
	}
	t.Run("canceled", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		var s auditedSignalIndex
		if err := s.append(ctx, packets[0], keys[0], 0); err != context.Canceled {
			t.Fatal(err)
		}
	})
}

type rawIndexReader [][]byte

func (r rawIndexReader) ReadIndex(_ context.Context, i uint64) ([]byte, error) {
	if i >= uint64(len(r)) {
		return nil, fmt.Errorf("missing packet")
	}
	return r[i], nil
}

func TestRawGraphSignalIndexProductionPackets(t *testing.T) {
	// Production only generates the inputs to this differential test; the
	// independent checker never calls its decoder, update or lookup.
	var packets rawIndexReader
	var audit auditedSignalIndex
	for i := 0; i < 1024; i++ {
		key := sha256.Sum256([]byte(fmt.Sprintf("independent-prefix-%d", i)))
		packet, _, found, err := retainedindex.Update(context.Background(), packets, uint64(i), retainedindex.Key(key), uint64(i))
		if err != nil || found {
			t.Fatal(i, found, err)
		}
		if err = audit.append(context.Background(), packet, key, uint64(i)); err != nil {
			t.Fatal(i, err)
		}
		packets = append(packets, packet)
	}
	t.Run("maximum-depth", func(t *testing.T) {
		var packets rawIndexReader
		var audit auditedSignalIndex
		for i := 0; i <= 256; i++ {
			var key [32]byte
			if i > 0 {
				bit := i - 1
				key[bit/8] = 0x80 >> uint(bit%8)
			}
			packet, _, found, err := retainedindex.Update(context.Background(), packets, uint64(i), retainedindex.Key(key), uint64(i))
			if err != nil || found {
				t.Fatal(i, found, err)
			}
			if err = audit.append(context.Background(), packet, key, uint64(i)); err != nil {
				t.Fatal(i, err)
			}
			packets = append(packets, packet)
		}
		if len(packets[256]) != 16+257*80 {
			t.Fatal("did not exercise maximum packet", len(packets[256]))
		}
	})
}
