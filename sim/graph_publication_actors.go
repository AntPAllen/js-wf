package sim

import (
	"context"

	"js-wf/internal/blobpublication"
	"js-wf/internal/graphpublication"
	"js-wf/internal/retainedgraph"
)

// GraphProtocolWithYield exposes every graph authority/object/catalog operation
// and identity allocation as an actor turn. The underlying model still applies
// its actual CAS, ownership and immutable-object rules. Prepared write inputs
// are copied before another actor can advance the shared store.
func GraphProtocolWithYield(model *GraphPublicationTransport, yield YieldFunc) graphpublication.Protocol {
	protocol := model.Protocol()
	newID := protocol.NewID
	protocol.NewID = func() (string, error) {
		return graphActorCall(context.Background(), yield, "graph_new_id", newID)
	}
	protocol.Port = yieldingGraphPort{GraphPublicationTransport: model, yield: yield}
	return protocol
}

type yieldingGraphPort struct {
	*GraphPublicationTransport
	yield YieldFunc
}

func graphActorCall[T any](ctx context.Context, yield YieldFunc, operation string, call func() (T, error)) (result T, err error) {
	if e := yield(ctx, operation, func() { result, err = call() }); e != nil {
		var zero T
		return zero, e
	}
	return result, err
}
func graphActorEffect(ctx context.Context, yield YieldFunc, operation string, call func() error) error {
	_, err := graphActorCall(ctx, yield, operation, func() (struct{}, error) { return struct{}{}, call() })
	return err
}
func (p yieldingGraphPort) ReadRoot(ctx context.Context, key string) (graphpublication.Root, error) {
	return graphActorCall(ctx, p.yield, "graph_read_root", func() (graphpublication.Root, error) { return p.GraphPublicationTransport.ReadRoot(ctx, key) })
}
func (p yieldingGraphPort) CASRoot(ctx context.Context, key string, head uint64, next graphpublication.Root) (graphpublication.Root, error) {
	next = graphRootCopy(next)
	return graphActorCall(ctx, p.yield, "graph_cas_root", func() (graphpublication.Root, error) {
		return p.GraphPublicationTransport.CASRoot(ctx, key, head, next)
	})
}
func (p yieldingGraphPort) ReadBlob(ctx context.Context, key string) (graphpublication.Record, error) {
	return graphActorCall(ctx, p.yield, "graph_read_blob", func() (graphpublication.Record, error) { return p.GraphPublicationTransport.ReadBlob(ctx, key) })
}
func (p yieldingGraphPort) CASBlob(ctx context.Context, key string, revision uint64, next graphpublication.Fence) (graphpublication.Record, error) {
	next = graphFenceCopy(next)
	return graphActorCall(ctx, p.yield, "graph_cas_blob", func() (graphpublication.Record, error) {
		return p.GraphPublicationTransport.CASBlob(ctx, key, revision, next)
	})
}
func (p yieldingGraphPort) Put(ctx context.Context, name string, data []byte) error {
	data = append([]byte(nil), data...)
	return graphActorEffect(ctx, p.yield, "graph_put", func() error { return p.GraphPublicationTransport.Put(ctx, name, data) })
}
func (p yieldingGraphPort) Get(ctx context.Context, link retainedgraph.Link, limit int) ([]byte, error) {
	return graphActorCall(ctx, p.yield, "graph_get", func() ([]byte, error) { return p.GraphPublicationTransport.Get(ctx, link, limit) })
}
func (p yieldingGraphPort) Delete(ctx context.Context, name string) error {
	return graphActorEffect(ctx, p.yield, "graph_delete", func() error { return p.GraphPublicationTransport.Delete(ctx, name) })
}
func (p yieldingGraphPort) BlobKeys(ctx context.Context) ([]string, error) {
	return graphActorCall(ctx, p.yield, "graph_blob_keys", func() ([]string, error) { return p.GraphPublicationTransport.BlobKeys(ctx) })
}
func (p yieldingGraphPort) Objects(ctx context.Context) ([]blobpublication.Object, error) {
	return graphActorCall(ctx, p.yield, "graph_objects", func() ([]blobpublication.Object, error) { return p.GraphPublicationTransport.Objects(ctx) })
}
func (p yieldingGraphPort) RootKeys(ctx context.Context) ([]string, error) {
	return graphActorCall(ctx, p.yield, "graph_root_keys", func() ([]string, error) { return p.GraphPublicationTransport.RootKeys(ctx) })
}
func (p yieldingGraphPort) NextRoot(ctx context.Context, next uint64) (*graphpublication.RootCatalogEntry, error) {
	return graphActorCall(ctx, p.yield, "graph_next_root", func() (*graphpublication.RootCatalogEntry, error) {
		return p.GraphPublicationTransport.NextRoot(ctx, next)
	})
}
func (p yieldingGraphPort) RootCatalogHighWater(ctx context.Context) (uint64, error) {
	return graphActorCall(ctx, p.yield, "graph_root_watermark", func() (uint64, error) { return p.GraphPublicationTransport.RootCatalogHighWater(ctx) })
}
