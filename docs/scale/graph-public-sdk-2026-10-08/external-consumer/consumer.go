package graphconsumer
import (
 "context"
 "github.com/nats-io/nats.go/jetstream"
 "js-wf/client"
 "js-wf/journal"
 "js-wf/worker"
)
func Open(ctx context.Context, js jetstream.JetStream, handlers map[string]worker.Handler) (*worker.Worker,*client.Client,error) {
 cfg := journal.NativeGraphConfig{AuthorityStream:"WF_GRAPH_AUTH", AuthorityPrefix:"wf.graph.runtime", ObjectBucket:"WF_GRAPH_OBJECTS",ExpectedReplicas:3}
 configs,err := journal.NativeGraphStreamConfigs(cfg,3)
 if err != nil {return nil,nil,err}
 _ = configs // Provision separately, never during runtime admission.
 store,err := journal.OpenNativeGraphStore(ctx,js,cfg)
 if err != nil {return nil,nil,err}
 w,err := worker.New(ctx,js,"public-worker",handlers,worker.WithGraphJournal(store))
 if err != nil {return nil,nil,err}
 c,err := client.NewWithGraphJournal(js,store)
 return w,c,err
}
func Reuse(ctx context.Context, store *journal.GraphStore, link journal.GraphPayloadLink, expected uint64) (uint64,error) {
 return store.Append(ctx,"flow","id",1,journal.Entry{Kind:journal.Completed,Index:1},expected,nil,[]journal.GraphOwnedPayload{{Index:0,Link:link}})
}
