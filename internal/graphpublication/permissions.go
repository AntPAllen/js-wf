package graphpublication

import (
	"errors"
	"strings"
	"unicode/utf8"

	"js-wf/internal/blobpublication"
)

// SubjectAccess requires default-deny NATS permissions. These roles are for
// trusted adapters; publish access can write arbitrary authority/object metadata.
type SubjectAccess = blobpublication.SubjectAccess

// NativeSubjectAccess permits the native graph Port's witnessed reads, CAS,
// physical message reads and uploads in one dedicated authority/object namespace.
// Get creates no consumers, so consumer lifecycle and flow-control grants are
// omitted. Provisioning uses separate credentials. apiPrefix must match the
// client's actual JetStream namespace; domain/account imports need admission.
// Collectors may purge only the named bucket's API subject. NATS permissions
// cannot constrain the JSON purge filter, so collector credentials must remain
// confined to the trusted adapter; administrative ownership remains separate.
func NativeSubjectAccess(name, prefix, apiPrefix, bucket string, collector bool) (SubjectAccess, error) {
	if !utf8.ValidString(name) || !utf8.ValidString(prefix) || !utf8.ValidString(apiPrefix) {
		return SubjectAccess{}, errors.New("invalid graph permission encoding")
	}
	access, err := blobpublication.AuthoritySubjectAccess(name, prefix, apiPrefix)
	if err != nil {
		return SubjectAccess{}, err
	}
	if !validID(strings.ReplaceAll(bucket, "_", "-")) {
		return SubjectAccess{}, errors.New("invalid graph permission bucket")
	}
	stream := "OBJ_" + bucket
	access.Publish = append(access.Publish, "$O."+bucket+".C.*", "$O."+bucket+".M.*", apiPrefix+".STREAM.INFO."+stream, apiPrefix+".STREAM.MSG.GET."+stream)
	if collector {
		access.Publish = append(access.Publish, apiPrefix+".STREAM.PURGE."+stream)
	}
	return access, nil
}
