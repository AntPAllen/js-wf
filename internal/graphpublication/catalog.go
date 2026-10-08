package graphpublication

import (
	"context"
	"errors"
	"sort"
	"time"
	"unicode/utf8"
)

// SweepWithReaders fences expired pins at every catalogued destination before
// object collection. This includes empty snapshots with no object grant, and
// destinations whose old object grants are all closed. Missing catalog support
// fails closed. The original Sweep remains the object-grant operation used by
// historical replay; canonical adoption must use this complete reader lifecycle.
func (p Protocol) SweepWithReaders(ctx context.Context, now time.Time) (int, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	if p.Port == nil || now.IsZero() {
		return 0, errors.New("port and reader collection time required")
	}
	catalog, ok := p.Port.(RootCatalogPort)
	if !ok {
		return 0, errors.New("graph port has no destination catalog")
	}
	keys, err := catalog.RootKeys(ctx)
	if err != nil {
		return 0, err
	}
	keys = append([]string(nil), keys...)
	sort.Strings(keys)
	for i, destination := range keys {
		if destination == "" || !utf8.ValidString(destination) || len(destination) > 256 || (i > 0 && keys[i-1] == destination) {
			return 0, errors.New("invalid graph destination catalog")
		}
	}
	for _, destination := range keys {
		if err = p.expireCatalogRoot(ctx, destination, now); err != nil {
			return 0, err
		}
	}
	return p.Sweep(ctx, now)
}

func (p Protocol) expireCatalogRoot(ctx context.Context, destination string, now time.Time) error {
	for attempts := 0; attempts < 32; attempts++ {
		root, err := p.readRoot(ctx, destination)
		if err != nil {
			return err
		}
		_, err = p.pruneReaders(ctx, destination, root, now)
		if errors.Is(err, ErrConflict) {
			continue
		}
		// Unknown expiry replies stop before grant closure or object deletion.
		return err
	}
	return ErrConflict
}
