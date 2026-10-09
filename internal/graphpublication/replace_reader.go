package graphpublication

import (
	"context"
	"errors"
	"time"
)

// ReplaceReader atomically exchanges an exact live pin for the current forest
// at expected. A fresh ID revokes the old handle even for an unchanged forest.
// This captures only canonical current ownership; it cannot adopt a caller's
// snapshot, resurrect an expired pin or shorten its existing retention lease.
// Unknown outcomes use readerCAS's exact original-head witness, never retry.
func (p Protocol) ReplaceReader(ctx context.Context, reader Reader, expected uint64, now func() time.Time, expires time.Time) (Reader, Root, error) {
	if now == nil || expires.IsZero() {
		return Reader{}, Root{}, errors.New("invalid replacement reader expiry")
	}
	root, err := p.readerRoot(ctx, reader.destination, expected)
	if err != nil {
		return Reader{}, Root{}, err
	}
	i, err := readerIndex(root, reader)
	if err != nil {
		return Reader{}, Root{}, err
	}
	current := now()
	if current.IsZero() || !current.Before(root.Readers[i].Expires) || !expires.After(current) {
		return Reader{}, Root{}, ErrRevoked
	}
	if expires.Before(root.Readers[i].Expires) {
		return Reader{}, Root{}, errors.New("replacement reader shortens expiry")
	}
	id, err := p.id()
	if err != nil {
		return Reader{}, Root{}, err
	}
	if !validID(id) {
		return Reader{}, Root{}, errors.New("invalid replacement reader ID")
	}
	for _, pin := range root.Readers {
		if pin.ID == id {
			return Reader{}, Root{}, errors.New("replacement reader ID reused")
		}
	}
	current = now()
	if current.IsZero() || !current.Before(root.Readers[i].Expires) || !expires.After(current) {
		return Reader{}, Root{}, ErrRevoked
	}
	next := Reader{destination: reader.destination, id: id, graph: copyGraph(root.Graph), streams: copyStreams(root.Streams)}
	root.Readers = copyReaders(root.Readers)
	root.Readers[i] = ReaderPin{ID: id, Expires: expires.UTC(), Graph: copyGraph(root.Graph), Streams: copyStreams(root.Streams)}
	ack, err := p.readerCAS(ctx, reader.destination, expected, root)
	if err != nil {
		return Reader{}, Root{}, err
	}
	return next, ack, nil
}
