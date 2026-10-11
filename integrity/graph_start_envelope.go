package integrity

import (
	"fmt"
	"js-wf/identity"
)

// Own complete canonical start wire declarations; production start admission
// and graph readers are not used to validate their own output.
type auditedGraphStartRequest struct {
	Type             string `json:"type"`
	ID               string `json:"id"`
	ParentType       string `json:"parent_type,omitempty"`
	ParentID         string `json:"parent_id,omitempty"`
	ParentInvocation uint64 `json:"parent_invocation,omitempty"`
	SignalName       string `json:"signal_name,omitempty"`
}
type auditedGraphStart struct {
	Schema      string                   `json:"schema"`
	Token       string                   `json:"token"`
	Request     auditedGraphStartRequest `json:"request"`
	InputSHA256 string                   `json:"input_sha256"`
	InputSize   int                      `json:"input_size"`
}

func auditStartParent(r auditedGraphStartRequest) error {
	if r.ParentType == "" && r.ParentID == "" && r.ParentInvocation == 0 && r.SignalName == "" {
		return nil
	}
	if identity.Validate(r.ParentType, r.ParentID) != nil || r.ParentInvocation == 0 || identity.ValidateToken(r.SignalName) != nil {
		return fmt.Errorf("invalid canonical start parent-return identity")
	}
	return nil
}
