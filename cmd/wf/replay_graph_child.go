package main

import "js-wf/wf"

// Validate before plugin loading; the shared SDK replay layer also validates
// exact child signal selection after every continuation restore.
func validateReplayGraphChildren(bundle replayBundle) error {
	return wf.ValidateReplayGraphChildren(bundle.Journal, bundle.Objects)
}
