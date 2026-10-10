package retention_test

import (
	"fmt"
	"js-wf/sim"
	"reflect"
	"testing"
)

func TestGraphRetentionWorkflowSDKCrashAndReuseReplay(t *testing.T) {
	for cut := 0; cut <= 4; cut++ {
		for _, reuse := range []bool{false, true} {
			if reuse && cut < 3 {
				continue
			}
			t.Run(fmt.Sprintf("cut%d/reuse%v", cut, reuse), func(t *testing.T) {
				for seed := int64(1); seed <= 16; seed++ {
					generated, err := sim.RunGraphRetentionWorkflowSDKCut(seed, cut, reuse, nil)
					if err != nil {
						t.Fatalf("seed=%d: %v", seed, err)
					}
					replayed, err := sim.RunGraphRetentionWorkflowSDKCut(seed, cut, reuse, &generated)
					if err != nil || !reflect.DeepEqual(generated, replayed) {
						t.Fatalf("seed=%d exact replay differs: %v", seed, err)
					}
				}
			})
		}
	}
}
