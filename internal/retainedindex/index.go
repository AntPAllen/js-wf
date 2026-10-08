// Package retainedindex packs a persistent crit-bit index into append-forest
// records. Every update copies only its search path, with at most 257 nodes.
// Locators refer to record positions in the same captured forest, never object
// names or another forest. This codec does not grant ownership: callers must
// read through a pinned graph view and publish the packet with its forest CAS.
package retainedindex

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"math"
)

const MaxNodes = 257
const nodeBytes = 80
const headerBytes = 16
const MaxPacketBytes = headerBytes + MaxNodes*nodeBytes
const magic = "JWFIDX01"

var ErrInvalid = errors.New("invalid retained key index")

type Key [32]byte

// Reader returns index packet bytes from the indicated owned forest record.
// Count supplied to Lookup/Update must come from that same captured forest.
type Reader interface {
	ReadIndex(context.Context, uint64) ([]byte, error)
}

type location struct {
	record uint64
	slot   uint16
}
type node struct {
	key         Key
	bit         uint16 // 256 denotes a leaf; branch keys contain only the prefix.
	value       uint64
	left, right location
}
type edge struct {
	loc   location
	node  node
	right bool
}
type search struct {
	ctx    context.Context
	reader Reader
	count  uint64
	cache  map[uint64][]node
}

func bit(k Key, n uint16) bool { return k[n/8]&(0x80>>uint(n%8)) != 0 }
func prefix(k Key, n uint16) Key {
	for i := n; i < 256; i++ {
		k[i/8] &^= 0x80 >> uint(i%8)
	}
	return k
}
func difference(a, b Key) uint16 {
	for i := uint16(0); i < 256; i++ {
		if bit(a, i) != bit(b, i) {
			return i
		}
	}
	return 256
}
func earlier(a, b location) bool {
	return a.record < b.record || a.record == b.record && a.slot < b.slot
}
func readLocation(b []byte) location {
	return location{binary.BigEndian.Uint64(b), binary.BigEndian.Uint16(b[8:])}
}
func writeLocation(b []byte, l location) {
	binary.BigEndian.PutUint64(b, l.record)
	binary.BigEndian.PutUint16(b[8:], l.slot)
}

func decode(data []byte, record uint64) ([]node, error) {
	if len(data) < headerBytes || len(data) > MaxPacketBytes || string(data[:8]) != magic || !bytes.Equal(data[10:16], make([]byte, 6)) {
		return nil, ErrInvalid
	}
	n := int(binary.BigEndian.Uint16(data[8:]))
	if n < 1 || n > MaxNodes || len(data) != headerBytes+n*nodeBytes {
		return nil, ErrInvalid
	}
	nodes := make([]node, n)
	for i := range nodes {
		b := data[headerBytes+i*nodeBytes : headerBytes+(i+1)*nodeBytes]
		v := &nodes[i]
		copy(v.key[:], b[:32])
		v.bit = binary.BigEndian.Uint16(b[32:])
		v.value = binary.BigEndian.Uint64(b[40:])
		v.left = readLocation(b[48:64])
		v.right = readLocation(b[64:80])
		if v.bit > 256 || !bytes.Equal(b[34:40], make([]byte, 6)) || !bytes.Equal(b[58:64], make([]byte, 6)) || !bytes.Equal(b[74:80], make([]byte, 6)) {
			return nil, ErrInvalid
		}
		if v.bit == 256 {
			if v.left != (location{}) || v.right != (location{}) {
				return nil, ErrInvalid
			}
		} else {
			self := location{record, uint16(i)}
			if v.value != 0 || v.key != prefix(v.key, v.bit) || v.left == v.right || !earlier(v.left, self) || !earlier(v.right, self) {
				return nil, ErrInvalid
			}
			for side, child := range []location{v.left, v.right} {
				if child.record == record && !validChild(*v, nodes[child.slot], side == 1) {
					return nil, ErrInvalid
				}
			}
		}
	}
	// Every new node must be reachable from the packet's last (root) slot.
	seen := make([]bool, n)
	var visit func(uint16)
	visit = func(i uint16) {
		if seen[i] {
			return
		}
		seen[i] = true
		v := nodes[i]
		if v.bit < 256 {
			for _, l := range []location{v.left, v.right} {
				if l.record == record {
					visit(l.slot)
				}
			}
		}
	}
	visit(uint16(n - 1))
	for _, ok := range seen {
		if !ok {
			return nil, ErrInvalid
		}
	}
	return nodes, nil
}
func validChild(parent, child node, right bool) bool {
	return child.bit > parent.bit && prefix(child.key, parent.bit) == parent.key && bit(child.key, parent.bit) == right
}
func encode(nodes []node) ([]byte, error) {
	if len(nodes) < 1 || len(nodes) > MaxNodes {
		return nil, ErrInvalid
	}
	data := make([]byte, headerBytes+len(nodes)*nodeBytes)
	copy(data, magic)
	binary.BigEndian.PutUint16(data[8:], uint16(len(nodes)))
	for i, v := range nodes {
		b := data[headerBytes+i*nodeBytes : headerBytes+(i+1)*nodeBytes]
		copy(b[:32], v.key[:])
		binary.BigEndian.PutUint16(b[32:], v.bit)
		binary.BigEndian.PutUint64(b[40:], v.value)
		writeLocation(b[48:64], v.left)
		writeLocation(b[64:80], v.right)
	}
	return data, nil
}
func (s *search) packet(record uint64) ([]node, error) {
	if err := s.ctx.Err(); err != nil {
		return nil, err
	}
	if record >= s.count || s.reader == nil {
		return nil, ErrInvalid
	}
	if v, ok := s.cache[record]; ok {
		return v, nil
	}
	if len(s.cache) >= MaxNodes {
		return nil, ErrInvalid
	}
	data, err := s.reader.ReadIndex(s.ctx, record)
	if err != nil {
		return nil, err
	}
	v, err := decode(data, record)
	if err == nil {
		s.cache[record] = v
	}
	return v, err
}
func (s *search) walk(key Key) ([]edge, location, node, error) {
	packet, err := s.packet(s.count - 1)
	if err != nil {
		return nil, location{}, node{}, err
	}
	loc := location{s.count - 1, uint16(len(packet) - 1)}
	path := []edge{}
	var parent *edge
	for {
		packet, err = s.packet(loc.record)
		if err != nil {
			return nil, loc, node{}, err
		}
		if int(loc.slot) >= len(packet) {
			return nil, loc, node{}, ErrInvalid
		}
		v := packet[loc.slot]
		if parent != nil && !validChild(parent.node, v, parent.right) {
			return nil, loc, node{}, ErrInvalid
		}
		if v.bit == 256 || prefix(key, v.bit) != v.key {
			return path, loc, v, nil
		}
		path = append(path, edge{loc, v, bit(key, v.bit)})
		if len(path) >= MaxNodes {
			return nil, loc, node{}, ErrInvalid
		}
		parent = &path[len(path)-1]
		loc = v.left
		if parent.right {
			loc = v.right
		}
	}
}
func newSearch(ctx context.Context, reader Reader, count uint64) (*search, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if count > math.MaxInt64 {
		return nil, ErrInvalid
	}
	return &search{ctx: ctx, reader: reader, count: count, cache: map[uint64][]node{}}, nil
}

// Lookup performs at most 257 bounded packet reads, independent of population.
// A missing/uncertain/corrupt referenced packet is an error, never key absence.
func Lookup(ctx context.Context, reader Reader, count uint64, key Key) (uint64, bool, error) {
	s, err := newSearch(ctx, reader, count)
	if err != nil {
		return 0, false, err
	}
	if count == 0 {
		return 0, false, nil
	}
	_, _, v, err := s.walk(key)
	if err != nil {
		return 0, false, err
	}
	if v.bit != 256 || v.key != key {
		return 0, false, nil
	}
	return v.value, true, nil
}

// Update prepares one packet for record index count. Publication must append
// it at that exact captured forest population. Equal key/value is a no-op
// (nil packet); otherwise the packet preserves all earlier indexed keys.
// The old value/found result lets callers enforce write-once input identity.
func Update(ctx context.Context, reader Reader, count uint64, key Key, value uint64) (packet []byte, old uint64, found bool, err error) {
	s, err := newSearch(ctx, reader, count)
	if err != nil || count == math.MaxInt64 {
		if err == nil {
			err = ErrInvalid
		}
		return nil, 0, false, err
	}
	nodes := []node{{key: key, bit: 256, value: value}}
	if count == 0 {
		packet, err = encode(nodes)
		return
	}
	path, loc, v, err := s.walk(key)
	if err != nil {
		return nil, 0, false, err
	}
	found = v.bit == 256 && v.key == key
	if found {
		old = v.value
		if old == value {
			return nil, old, true, nil
		}
	} else {
		d := difference(key, v.key)
		if d >= v.bit {
			return nil, 0, false, ErrInvalid
		}
		branch := node{key: prefix(key, d), bit: d, left: location{count, 0}, right: loc}
		if bit(key, d) {
			branch.left, branch.right = branch.right, branch.left
		}
		nodes = append(nodes, branch)
	}
	for i := len(path) - 1; i >= 0; i-- {
		parent := path[i].node
		child := location{count, uint16(len(nodes) - 1)}
		if path[i].right {
			parent.right = child
		} else {
			parent.left = child
		}
		nodes = append(nodes, parent)
	}
	packet, err = encode(nodes)
	return
}
