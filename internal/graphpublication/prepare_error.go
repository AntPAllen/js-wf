package graphpublication

import "fmt"

// AppendPrepareError identifies where preparation stopped. It preserves the
// original error classification; a staged object never proves publication.
type AppendPrepareError struct {
	Phase string
	Err   error
}

func (e *AppendPrepareError) Error() string { return fmt.Sprintf("%s: %v", e.Phase, e.Err) }
func (e *AppendPrepareError) Unwrap() error { return e.Err }

// AcquisitionError describes a failed grant or immutable upload operation.
// It does not authorize retrying an uncertain write or adopting uploaded bytes.
type AcquisitionError struct {
	Phase string
	Err   error
}

func (e *AcquisitionError) Error() string { return fmt.Sprintf("%s: %v", e.Phase, e.Err) }
func (e *AcquisitionError) Unwrap() error { return e.Err }
