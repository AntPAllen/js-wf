package graphpublication

import "fmt"

// AppendCommitError identifies the failed commit phase without changing its
// primary error classification. ReadbackErr is diagnostic: it is not unwrapped
// because Commit has historically returned the original publication error.
type AppendCommitError struct {
	Phase             string
	Err               error
	ReadbackAttempted bool
	ReadbackErr       error
}

func (e *AppendCommitError) Error() string {
	if e.ReadbackAttempted && e.ReadbackErr != nil {
		return fmt.Sprintf("%s: %v (readback: %v)", e.Phase, e.Err, e.ReadbackErr)
	}
	return fmt.Sprintf("%s: %v", e.Phase, e.Err)
}
func (e *AppendCommitError) Unwrap() error { return e.Err }
func appendCommitError(phase string, err error) error {
	return &AppendCommitError{Phase: phase, Err: err}
}
