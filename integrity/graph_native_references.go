package integrity

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"strings"

	"github.com/nats-io/nats.go/jetstream"
	"js-wf/internal/graphpublication"
)

// GraphAuditNamespace selects explicitly provisioned, isolated graph stores.
// This reader never initializes authority, writes witnesses or acquires pins.
type GraphAuditNamespace struct {
	AuthorityStream, AuthorityPrefix, ObjectBucket string
	PayloadLimit                                   int
}

// CheckNativeGraphReferences audits a quiescent namespace from retained raw
// authority messages and physical ObjectStore bytes. Administrative reads are
// observational evidence, not the runtime's linearizable admission authority.
// The caller must stop all writers, readers, collectors and retention/reuse for
// the whole audit. A changed authority high-water mark rejects the observation;
// stability alone does not certify quiescence, quorum or VM disk durability.
func CheckNativeGraphReferences(ctx context.Context, js jetstream.JetStream, ns GraphAuditNamespace) (GraphReferenceReport, error) {
	var report GraphReferenceReport
	if js == nil || ns.AuthorityStream == "" || ns.AuthorityPrefix == "" || ns.ObjectBucket == "" || ns.PayloadLimit <= 0 || ns.PayloadLimit == math.MaxInt {
		return report, fmt.Errorf("invalid native graph audit namespace/bounds")
	}
	stream, err := js.Stream(ctx, ns.AuthorityStream)
	if err != nil {
		return report, err
	}
	before, err := stream.Info(ctx)
	if err != nil {
		return report, err
	}
	c := before.Config
	indexed := c.Metadata["wf_graph_owner_index"] == "js-wf-graph-owner-index-v1"
	subjectsOK := len(c.Subjects) == 1 && c.Subjects[0] == ns.AuthorityPrefix+".>" && c.Metadata["wf_graph_owner_index"] == ""
	if indexed {
		subjectsOK = len(c.Subjects) == 3 && c.Subjects[0] == ns.AuthorityPrefix+".root.>" && c.Subjects[1] == ns.AuthorityPrefix+".blob.>" && c.Subjects[2] == ns.AuthorityPrefix+".owner.>"
	}
	if !subjectsOK || c.MaxMsgsPerSubject != 1 || c.Retention != jetstream.LimitsPolicy || c.Discard != jetstream.DiscardOld || !c.DenyDelete || !c.DenyPurge || c.MaxAge != 0 || c.MaxMsgs > 0 || c.MaxBytes > 0 {
		return report, fmt.Errorf("unsafe graph audit authority config")
	}
	snapshot := GraphReferenceSnapshot{Roots: map[string]graphpublication.Root{}, Fences: map[string]graphpublication.Fence{}, PayloadLimit: ns.PayloadLimit}
	owners := map[string]string{}
	seen := map[string]bool{}
	var count uint64
	err = scan(ctx, stream, func(message *jetstream.RawStreamMsg) error {
		if message.Sequence == 0 || message.Sequence > before.State.LastSeq || seen[message.Subject] {
			return fmt.Errorf("graph audit duplicate/out-of-bound authority subject")
		}
		seen[message.Subject] = true
		count++
		if len(message.Data) > graphpublication.MaxAuthorityBytes {
			return fmt.Errorf("oversized graph authority")
		}
		if strings.HasPrefix(message.Subject, ns.AuthorityPrefix+".owner.") {
			var marker struct{ Schema, Owner, Scope string }
			if !indexed || json.Unmarshal(message.Data, &marker) != nil || marker.Schema != "js-wf-graph-owner-index-v1" || !graphAuditID(marker.Owner) || !graphAuditHash(marker.Scope) || message.Subject != ns.AuthorityPrefix+".owner."+digest([]byte(marker.Owner))+"."+marker.Scope {
				return fmt.Errorf("invalid graph owner index marker")
			}
			if prior, ok := owners[marker.Scope]; ok && prior != marker.Owner {
				return fmt.Errorf("graph owner index scope collision")
			}
			owners[marker.Scope] = marker.Owner
			return nil
		}
		var value struct {
			Schema, Kind, Identity string
			Revision               uint64
			Root                   *graphpublication.Root
			Fence                  *graphpublication.Fence
		}
		if json.Unmarshal(message.Data, &value) != nil || value.Schema != "js-wf-graph-authority-v1" || value.Identity == "" {
			return fmt.Errorf("invalid raw graph authority envelope")
		}
		identity := value.Identity
		switch value.Kind {
		case "root":
			identity = digest([]byte(identity))
		case "blob":
			if !graphAuditHash(identity) {
				return fmt.Errorf("invalid graph scope hash")
			}
		default:
			return fmt.Errorf("unknown graph authority kind")
		}
		if message.Subject != ns.AuthorityPrefix+"."+value.Kind+"."+identity {
			return fmt.Errorf("graph authority subject/identity mismatch")
		}
		if value.Revision == 0 {
			if value.Root != nil || value.Fence != nil {
				return fmt.Errorf("invalid graph absence witness")
			}
			return nil
		}
		if value.Kind == "root" {
			if value.Root == nil || value.Fence != nil || value.Root.Head != value.Revision {
				return fmt.Errorf("invalid graph root revision")
			}
			snapshot.Roots[value.Identity] = *value.Root
		} else {
			if value.Fence == nil || value.Root != nil || graphAuditFence(value.Identity, *value.Fence) != nil {
				return fmt.Errorf("invalid graph fence")
			}
			snapshot.Fences[value.Identity] = *value.Fence
		}
		return nil
	})
	if err != nil {
		return report, err
	}
	if count != before.State.Msgs {
		return report, fmt.Errorf("graph authority census changed/lost records")
	}
	if indexed {
		for scope, fence := range snapshot.Fences {
			if owners[scope] != fence.Owner {
				return report, fmt.Errorf("graph scope lacks durable owner index marker")
			}
		}
	}
	objects, err := js.ObjectStore(ctx, ns.ObjectBucket)
	if err != nil {
		return report, err
	}
	objectStream, err := js.Stream(ctx, "OBJ_"+ns.ObjectBucket)
	if err != nil {
		return report, err
	}
	objectBefore, err := objectStream.Info(ctx)
	if err != nil {
		return report, err
	}
	oc := objectBefore.Config
	if oc.Metadata["js-wf-blob-format"] != "graph-recoverable-v1" || len(oc.Subjects) != 2 || oc.Subjects[0] != "$O."+ns.ObjectBucket+".C.>" || oc.Subjects[1] != "$O."+ns.ObjectBucket+".M.>" || oc.Retention != jetstream.LimitsPolicy || oc.Discard != jetstream.DiscardOld || oc.MaxAge != 0 || oc.MaxMsgs > 0 || oc.MaxBytes > 0 || !oc.AllowRollup || !oc.DenyDelete {
		return report, fmt.Errorf("unsafe graph audit object config")
	}
	snapshot.LoadObject = func(call context.Context, name string, limit int) ([]byte, error) {
		result, err := objects.Get(call, name)
		if err != nil {
			return nil, err
		}
		defer result.Close()
		info, err := result.Info()
		if err != nil {
			return nil, err
		}
		if info.Name != name || info.Deleted || info.Size > uint64(limit) || info.Metadata["js-wf-graph-state"] != "complete" {
			return nil, fmt.Errorf("invalid/oversized raw graph object metadata")
		}
		data, err := io.ReadAll(io.LimitReader(result, int64(limit)+1))
		if err != nil {
			return nil, err
		}
		if len(data) > limit || uint64(len(data)) != info.Size {
			return nil, fmt.Errorf("raw graph object size changed")
		}
		return data, nil
	}
	report, err = CheckGraphReferences(ctx, snapshot)
	if err != nil {
		return report, err
	}
	after, err := stream.Info(ctx)
	if err != nil {
		return report, err
	}
	if before.State.LastSeq != after.State.LastSeq || before.State.FirstSeq != after.State.FirstSeq || before.State.Msgs != after.State.Msgs {
		return report, fmt.Errorf("graph authority changed during raw audit")
	}
	objectAfter, err := objectStream.Info(ctx)
	if err != nil {
		return report, err
	}
	if objectBefore.State.LastSeq != objectAfter.State.LastSeq || objectBefore.State.FirstSeq != objectAfter.State.FirstSeq || objectBefore.State.Msgs != objectAfter.State.Msgs {
		return report, fmt.Errorf("graph objects changed during raw audit")
	}
	return report, nil
}
