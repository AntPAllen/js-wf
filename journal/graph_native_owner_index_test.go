package journal_test

import (
	"context"
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	"js-wf/journal"
	"js-wf/testcluster"
)

func TestNativeGraphOwnerIndexModeAdmission(t *testing.T) {
	for _, indexed := range []bool{false, true} {
		t.Run(fmt.Sprintf("indexed=%t", indexed), func(t *testing.T) {
			cluster, err := testcluster.Start(t.TempDir(), 1)
			if err != nil {
				t.Fatal(err)
			}
			defer cluster.Close()
			c, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			js, err := jetstream.New(cluster.Clients[0])
			if err != nil {
				t.Fatal(err)
			}
			cfg := journal.NativeGraphConfig{AuthorityStream: "MODE_AUTH", AuthorityPrefix: "wf.mode", ObjectBucket: "MODE_OBJECTS", OwnerScopeIndex: indexed, ExpectedReplicas: 1}
			configs, err := journal.NativeGraphStreamConfigs(cfg, 1)
			if err != nil {
				t.Fatal(err)
			}
			for _, config := range configs {
				if _, err = js.CreateStream(c, config); err != nil {
					t.Fatal(err)
				}
			}
			if _, err = journal.OpenNativeGraphStore(c, js, cfg); err != nil {
				t.Fatal("matching mode rejected", err)
			}
			before := make([]*jetstream.StreamInfo, len(configs))
			for i, config := range configs {
				s, e := js.Stream(c, config.Name)
				if e != nil {
					t.Fatal(e)
				}
				before[i], e = s.Info(c)
				if e != nil {
					t.Fatal(e)
				}
			}
			wrong := cfg
			wrong.OwnerScopeIndex = !indexed
			if _, err = journal.OpenNativeGraphStore(c, js, wrong); err == nil {
				t.Fatal("wrong mode admitted")
			}
			for i, config := range configs {
				s, e := js.Stream(c, config.Name)
				if e != nil {
					t.Fatal(e)
				}
				after, e := s.Info(c)
				if e != nil || !reflect.DeepEqual(before[i].Config, after.Config) || before[i].State.LastSeq != after.State.LastSeq {
					t.Fatal("mode rejection changed store", config.Name, e)
				}
			}
		})
	}
}
