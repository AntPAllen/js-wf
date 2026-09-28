package wf

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"strconv"
	"time"
)

var ErrInvocationIdentity = errors.New("workflow invocation identity is not configured")

// RunOnce passes a stable deduplication key to the effect. The external system
// must enforce that key; the workflow can still call the effect again after a
// crash before its StepCompleted append.
func RunOnce[T any](c *Context, name string, input any, fn func(context.Context, string) (T, error)) (T, error) {
	var zero T
	if c.parentType == "" || c.parentID == "" || c.parentInvSeq == 0 {
		return zero, ErrInvocationIdentity
	}
	material := c.parentType + ":" + c.parentID + ":" + strconv.FormatUint(c.parentInvSeq, 10) + ":" + strconv.FormatUint(uint64(c.position), 10) + ":" + name
	digest := sha256.Sum256([]byte(material))
	key := "wf-" + hex.EncodeToString(digest[:])
	declared := struct {
		Input any    `json:"input"`
		Key   string `json:"dedup_key"`
	}{Input: input, Key: key}
	return runWithKind(c, "run_once", name, declared, func(ctx context.Context) (T, error) { return fn(ctx, key) })
}

// Now journals one server timestamp, so replay observes the same time.
func Now(c *Context) (time.Time, error) {
	return Run(c, "__wf_now", nil, func(ctx context.Context) (time.Time, error) {
		if c.timerNow == nil {
			return time.Time{}, fmt.Errorf("server clock is not configured")
		}
		return c.timerNow(ctx)
	})
}

// Random journals one cryptographically generated uint64 for replay.
func Random(c *Context) (uint64, error) {
	return Run(c, "__wf_random", nil, func(context.Context) (uint64, error) {
		var bytes [8]byte
		if _, err := rand.Read(bytes[:]); err != nil {
			return 0, err
		}
		return binary.BigEndian.Uint64(bytes[:]), nil
	})
}
