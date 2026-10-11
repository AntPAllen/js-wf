package integrity

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
)

// Independent wire decoder and persistent-tree audit. No retainedindex codec,
// Update, Lookup or production graph reader participates in this check.
type auditedIndexLocation struct {
	record uint64
	slot   uint16
}
type auditedIndexNode struct {
	key         [32]byte
	bit         uint16
	value       uint64
	left, right auditedIndexLocation
	hash        [32]byte
}
type auditedSignalIndex struct{ packets [][]auditedIndexNode }

func auditedIndexBit(key [32]byte, bit uint16) bool { return key[bit/8]&(0x80>>(bit%8)) != 0 }
func auditedIndexPrefix(key [32]byte, bit uint16) [32]byte {
	for i := bit; i < 256; i++ {
		key[i/8] &^= 0x80 >> (i % 8)
	}
	return key
}
func auditedIndexHash(n auditedIndexNode, left, right [32]byte) [32]byte {
	// Logical identity deliberately excludes storage coordinates: copied paths
	// and old subtrees with identical meaning have the same fingerprint.
	var data [106]byte
	copy(data[:32], n.key[:])
	binary.BigEndian.PutUint16(data[32:34], n.bit)
	binary.BigEndian.PutUint64(data[34:42], n.value)
	copy(data[42:74], left[:])
	copy(data[74:], right[:])
	return sha256.Sum256(data[:])
}

func (s *auditedSignalIndex) append(ctx context.Context, data []byte, expected [32]byte, value uint64) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	bad := func() error { return fmt.Errorf("invalid canonical signal index packet") }
	if value != uint64(len(s.packets)) || len(data) < 96 || len(data) > 16+257*80 || string(data[:8]) != "JWFIDX01" || !bytes.Equal(data[10:16], make([]byte, 6)) {
		return bad()
	}
	count := int(binary.BigEndian.Uint16(data[8:10]))
	if count < 1 || count > 257 || len(data) != 16+count*80 {
		return bad()
	}
	nodes := make([]auditedIndexNode, count)
	location := func(b []byte) auditedIndexLocation {
		return auditedIndexLocation{binary.BigEndian.Uint64(b[:8]), binary.BigEndian.Uint16(b[8:10])}
	}
	get := func(loc auditedIndexLocation, slot int) (auditedIndexNode, bool) {
		if loc.record > value || loc.record == value && int(loc.slot) >= slot {
			return auditedIndexNode{}, false
		}
		if loc.record == value {
			return nodes[loc.slot], true
		}
		if loc.record >= uint64(len(s.packets)) || int(loc.slot) >= len(s.packets[loc.record]) {
			return auditedIndexNode{}, false
		}
		return s.packets[loc.record][loc.slot], true
	}
	leaves := 0
	for i := range nodes {
		b := data[16+i*80 : 16+(i+1)*80]
		n := &nodes[i]
		copy(n.key[:], b[:32])
		n.bit = binary.BigEndian.Uint16(b[32:34])
		n.value = binary.BigEndian.Uint64(b[40:48])
		n.left, n.right = location(b[48:64]), location(b[64:80])
		if n.bit > 256 || !bytes.Equal(b[34:40], make([]byte, 6)) || !bytes.Equal(b[58:64], make([]byte, 6)) || !bytes.Equal(b[74:80], make([]byte, 6)) {
			return bad()
		}
		if n.bit == 256 {
			if n.key != expected || n.value != value || n.left != (auditedIndexLocation{}) || n.right != (auditedIndexLocation{}) {
				return bad()
			}
			leaves++
			n.hash = auditedIndexHash(*n, [32]byte{}, [32]byte{})
		} else {
			if n.value != 0 || n.key != auditedIndexPrefix(n.key, n.bit) || n.left == n.right {
				return bad()
			}
			left, ok := get(n.left, i)
			if !ok {
				return bad()
			}
			right, ok := get(n.right, i)
			if !ok {
				return bad()
			}
			for side, child := range []auditedIndexNode{left, right} {
				if child.bit <= n.bit || auditedIndexPrefix(child.key, n.bit) != n.key || auditedIndexBit(child.key, n.bit) != (side == 1) {
					return bad()
				}
			}
			n.hash = auditedIndexHash(*n, left.hash, right.hash)
		}
	}
	if leaves != 1 {
		return bad()
	}
	// Every new node must belong to this update's last-slot root.
	seen := make([]bool, count)
	var visit func(int)
	visit = func(slot int) {
		if seen[slot] {
			return
		}
		seen[slot] = true
		n := nodes[slot]
		if n.bit == 256 {
			return
		}
		for _, child := range []auditedIndexLocation{n.left, n.right} {
			if child.record == value {
				visit(int(child.slot))
			}
		}
	}
	visit(count - 1)
	for _, yes := range seen {
		if !yes {
			return bad()
		}
	}
	// Remove only the newly indexed key from the current logical tree. The
	// remaining fingerprint must equal the preceding complete prefix tree.
	// This catches validly encoded updates which silently drop older keys.
	var remove func(auditedIndexNode) ([32]byte, bool, error)
	remove = func(n auditedIndexNode) ([32]byte, bool, error) {
		if err := ctx.Err(); err != nil {
			return [32]byte{}, false, err
		}
		if n.bit == 256 {
			if n.key != expected || n.value != value {
				return [32]byte{}, false, bad()
			}
			return [32]byte{}, true, nil
		}
		if auditedIndexPrefix(expected, n.bit) != n.key {
			return [32]byte{}, false, bad()
		}
		left, _ := get(n.left, count)
		right, _ := get(n.right, count)
		if auditedIndexBit(expected, n.bit) {
			hash, empty, err := remove(right)
			if err != nil {
				return hash, false, err
			}
			if empty {
				return left.hash, false, nil
			}
			return auditedIndexHash(n, left.hash, hash), false, nil
		}
		hash, empty, err := remove(left)
		if err != nil {
			return hash, false, err
		}
		if empty {
			return right.hash, false, nil
		}
		return auditedIndexHash(n, hash, right.hash), false, nil
	}
	remaining, empty, err := remove(nodes[count-1])
	if err != nil {
		return err
	}
	if value == 0 {
		if !empty || count != 1 {
			return bad()
		}
	} else {
		previous := s.packets[value-1]
		if empty || remaining != previous[len(previous)-1].hash {
			return fmt.Errorf("canonical signal index lost or altered prefix")
		}
	}
	s.packets = append(s.packets, nodes)
	return nil
}
