package blobpublication

import (
	"errors"
	"strings"
)

// SubjectAccess is a NATS client permission allowlist. Unlisted subjects must
// remain denied when installing it; do not combine it with publish/subscribe >.
type SubjectAccess struct {
	Publish   []string
	Subscribe []string
}

// AuthoritySubjectAccess permits authority reads, conditional writes and census,
// but no stream creation, deletion, update, purge or individual message deletion.
// apiPrefix is the client's actual JetStream API namespace (normally $JS.API).
// This is for trusted protocol adapters: authority publishers can still write
// arbitrary metadata. Provisioning administrators require separate credentials.
// ObjectStore and workflow-stream access are separate policies.
func AuthoritySubjectAccess(name, prefix, apiPrefix string) (SubjectAccess, error) {
	valid := func(s string) bool {
		return s != "" && !strings.ContainsAny(s, "*> /\\\t\r\n") && !strings.HasPrefix(s, ".") && !strings.HasSuffix(s, ".") && !strings.Contains(s, "..")
	}
	if !valid(name) || strings.Contains(name, ".") || !valid(prefix) || !valid(apiPrefix) {
		return SubjectAccess{}, errors.New("invalid authority permission namespace")
	}
	return SubjectAccess{
		Publish:   []string{prefix + ".root.*", prefix + ".blob.*", apiPrefix + ".STREAM.INFO." + name, apiPrefix + ".STREAM.MSG.GET." + name},
		Subscribe: []string{"_INBOX.>"},
	}, nil
}
