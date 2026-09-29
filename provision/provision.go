package provision

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

const Partitions uint32 = 64

// LeaseTTL leaves room for a worker-kill takeover inside the 30-second fault
// recovery gate. Workers renew every five seconds; an older bucket with a
// different TTL fails the exact configuration check below.
const LeaseTTL = 20 * time.Second

type TimerBackend string

const (
	NativeTimers   TimerBackend = "native"
	FallbackTimers TimerBackend = "fallback"
)

// EnsureAuto uses the configured WF_RUN mode when the stream exists. For a
// fresh deployment it selects native scheduling on NATS 2.12+ and the retained
// timer fallback on older servers. It fails closed while disconnected or when
// an existing native stream is reached through an older server.
func EnsureAuto(ctx context.Context, js jetstream.JetStream, replicas int) (TimerBackend, error) {
	return ensureAuto(ctx, js, replicas, 0)
}

// EnsureAutoWithJournalLimit provisions an exact WF_JRN byte cap and selects
// the timer backend in the same way as EnsureAuto. A cap must be positive.
func EnsureAutoWithJournalLimit(ctx context.Context, js jetstream.JetStream, replicas int, maxBytes int64) (TimerBackend, error) {
	if maxBytes <= 0 {
		return "", fmt.Errorf("journal max bytes must be positive")
	}
	return ensureAuto(ctx, js, replicas, maxBytes)
}

func ensureAuto(ctx context.Context, js jetstream.JetStream, replicas int, journalMaxBytes int64) (TimerBackend, error) {
	version := js.Conn().ConnectedServerVersion()
	nativeCapable, err := supportsSchedules(version)
	if err != nil {
		return "", err
	}
	backend := FallbackTimers
	run, err := js.Stream(ctx, "WF_RUN")
	switch {
	case err == nil:
		info, infoErr := run.Info(ctx)
		if infoErr != nil {
			return "", infoErr
		}
		if info.Config.AllowMsgSchedules {
			backend = NativeTimers
		}
	case errors.Is(err, jetstream.ErrStreamNotFound):
		if nativeCapable {
			backend = NativeTimers
		}
	default:
		return "", err
	}
	if backend == NativeTimers && !nativeCapable {
		return "", fmt.Errorf("native timer stream requires NATS 2.12+, connected to %s", version)
	}
	if backend == NativeTimers {
		err = ensure(ctx, js, replicas, true, journalMaxBytes)
	} else {
		err = ensure(ctx, js, replicas, false, journalMaxBytes)
	}
	if err != nil {
		return "", err
	}
	return backend, nil
}

func supportsSchedules(version string) (bool, error) {
	parts := strings.Split(strings.TrimPrefix(version, "v"), ".")
	if len(parts) < 3 {
		return false, fmt.Errorf("cannot determine NATS version %q", version)
	}
	major, err := strconv.Atoi(parts[0])
	if err != nil {
		return false, fmt.Errorf("cannot determine NATS version %q: %w", version, err)
	}
	minor, err := strconv.Atoi(parts[1])
	if err != nil {
		return false, fmt.Errorf("cannot determine NATS version %q: %w", version, err)
	}
	patch, _, _ := strings.Cut(parts[2], "-")
	patch, _, _ = strings.Cut(patch, "+")
	if _, err := strconv.Atoi(patch); err != nil {
		return false, fmt.Errorf("cannot determine NATS version %q: %w", version, err)
	}
	if major < 2 {
		return false, nil
	}
	return major > 2 || minor >= 12, nil
}

// Ensure creates the stores and rejects incompatible existing configurations.
// Calling CreateOrUpdateStream alone could silently adopt a destructive config.
func Ensure(ctx context.Context, js jetstream.JetStream, replicas int) error {
	return ensure(ctx, js, replicas, true, 0)
}

// EnsureWithJournalLimit provisions native timers and an exact WF_JRN byte cap.
// Existing deployments with a different cap are rejected rather than changed.
func EnsureWithJournalLimit(ctx context.Context, js jetstream.JetStream, replicas int, maxBytes int64) error {
	if maxBytes <= 0 {
		return fmt.Errorf("journal max bytes must be positive")
	}
	return ensure(ctx, js, replicas, true, maxBytes)
}

// EnsureFallback provisions retained timer records for servers without native
// message scheduling. The fallback poller must run alongside workers.
func EnsureFallback(ctx context.Context, js jetstream.JetStream, replicas int) error {
	return ensure(ctx, js, replicas, false, 0)
}

// EnsureFallbackWithJournalLimit provisions retained timers and an exact
// WF_JRN byte cap for servers without native message scheduling.
func EnsureFallbackWithJournalLimit(ctx context.Context, js jetstream.JetStream, replicas int, maxBytes int64) error {
	if maxBytes <= 0 {
		return fmt.Errorf("journal max bytes must be positive")
	}
	return ensure(ctx, js, replicas, false, maxBytes)
}

func ensure(ctx context.Context, js jetstream.JetStream, replicas int, nativeSchedules bool, journalMaxBytes int64) error {
	if replicas < 1 || replicas > 5 {
		return fmt.Errorf("replicas must be between 1 and 5")
	}
	streams := []jetstream.StreamConfig{
		{Name: "WF_INV", Subjects: []string{"wf.inv.*.*"}, Retention: jetstream.LimitsPolicy, Storage: jetstream.FileStorage, Replicas: replicas, MaxMsgsPerSubject: 1, Discard: jetstream.DiscardNew, DiscardNewPerSubject: true, MaxAge: 0},
		{Name: "WF_RUN", Subjects: []string{"wf.run.*", "wf.schedule.*.*.*"}, Retention: jetstream.WorkQueuePolicy, Storage: jetstream.FileStorage, Replicas: replicas, Discard: jetstream.DiscardOld, MaxAge: 0, AllowMsgSchedules: nativeSchedules, AllowRollup: true},
		{Name: "WF_JRN", Subjects: []string{"wf.jrn.*.*"}, Retention: jetstream.LimitsPolicy, Storage: jetstream.FileStorage, Replicas: replicas, Discard: jetstream.DiscardNew, MaxAge: 0, MaxBytes: journalMaxBytes},
		{Name: "WF_SIG", Subjects: []string{"wf.sig.*.*.*"}, Retention: jetstream.LimitsPolicy, Storage: jetstream.FileStorage, Replicas: replicas, Discard: jetstream.DiscardNew, MaxAge: 0, Duplicates: 2 * time.Minute},
		{Name: "WF_PURGE", Subjects: []string{"wf.purge.*.*"}, Retention: jetstream.WorkQueuePolicy, Storage: jetstream.FileStorage, Replicas: replicas, Discard: jetstream.DiscardOld, MaxAge: 30 * 24 * time.Hour, Duplicates: 2 * time.Minute},
	}
	if !nativeSchedules {
		streams = append(streams, jetstream.StreamConfig{Name: "WF_TIMER", Subjects: []string{"wf.timer.*.*.*.*"}, Retention: jetstream.LimitsPolicy, Storage: jetstream.FileStorage, Replicas: replicas, Discard: jetstream.DiscardNew, MaxAge: 0, Duplicates: 2 * time.Minute})
	}
	for _, desired := range streams {
		stream, err := js.Stream(ctx, desired.Name)
		if err == nil {
			info, err := stream.Info(ctx)
			if err != nil {
				return err
			}
			if err := matchStream(desired, info.Config); err != nil {
				return err
			}
			continue
		}
		if !errors.Is(err, jetstream.ErrStreamNotFound) {
			return err
		}
		if _, err := js.CreateStream(ctx, desired); err != nil {
			// Another process may have won the create race; verify what exists.
			stream, getErr := js.Stream(ctx, desired.Name)
			if getErr != nil {
				return err
			}
			info, getErr := stream.Info(ctx)
			if getErr != nil {
				return getErr
			}
			if getErr := matchStream(desired, info.Config); getErr != nil {
				return getErr
			}
		}
	}
	for _, cfg := range []jetstream.KeyValueConfig{
		{Bucket: "WF_LEASE", History: 1, TTL: LeaseTTL, LimitMarkerTTL: time.Minute, Storage: jetstream.FileStorage, Replicas: replicas},
		{Bucket: "WF_STATE", History: 1, Storage: jetstream.FileStorage, Replicas: replicas},
		{Bucket: "WF_VIEW", History: 1, Storage: jetstream.FileStorage, Replicas: replicas},
		{Bucket: "WF_ASSIGN", History: 1, Storage: jetstream.FileStorage, Replicas: replicas},
	} {
		kv, err := js.KeyValue(ctx, cfg.Bucket)
		if errors.Is(err, jetstream.ErrBucketNotFound) {
			kv, err = js.CreateKeyValue(ctx, cfg)
			if err != nil {
				kv, err = js.KeyValue(ctx, cfg.Bucket)
			}
		}
		if err != nil {
			return fmt.Errorf("bucket %s: %w", cfg.Bucket, err)
		}
		status, err := kv.Status(ctx)
		if err != nil {
			return err
		}
		if status.History() != int64(cfg.History) || status.TTL() != cfg.TTL || status.LimitMarkerTTL() != cfg.LimitMarkerTTL || status.Config().Replicas != cfg.Replicas || status.Config().Storage != cfg.Storage || status.Config().MaxBytes > 0 {
			return fmt.Errorf("bucket %s configuration mismatch", cfg.Bucket)
		}
	}
	objects, err := js.ObjectStore(ctx, "WF_BLOB")
	if errors.Is(err, jetstream.ErrBucketNotFound) {
		objects, err = js.CreateObjectStore(ctx, jetstream.ObjectStoreConfig{Bucket: "WF_BLOB", Storage: jetstream.FileStorage, Replicas: replicas})
		if err != nil {
			objects, err = js.ObjectStore(ctx, "WF_BLOB")
		}
	}
	if err != nil {
		return fmt.Errorf("object store WF_BLOB: %w", err)
	}
	objectStatus, err := objects.Status(ctx)
	if err != nil {
		return err
	}
	if objectStatus.Storage() != jetstream.FileStorage || objectStatus.Replicas() != replicas || objectStatus.TTL() != 0 {
		return fmt.Errorf("object store WF_BLOB configuration mismatch")
	}
	return nil
}

func matchStream(want, got jetstream.StreamConfig) error {
	wantMaxPerSubject := want.MaxMsgsPerSubject
	if wantMaxPerSubject == 0 {
		wantMaxPerSubject = -1
	}
	defaultUnlimited := func(n int64) int64 {
		if n == 0 {
			return -1
		}
		return n
	}
	defaultMsgSize := func(n int32) int32 {
		if n == 0 {
			return -1
		}
		return n
	}
	if !reflect.DeepEqual(want.Subjects, got.Subjects) || want.Retention != got.Retention || want.Storage != got.Storage || want.Replicas != got.Replicas || wantMaxPerSubject != got.MaxMsgsPerSubject || defaultUnlimited(want.MaxMsgs) != got.MaxMsgs || defaultUnlimited(want.MaxBytes) != got.MaxBytes || defaultMsgSize(want.MaxMsgSize) != got.MaxMsgSize || want.Discard != got.Discard || want.DiscardNewPerSubject != got.DiscardNewPerSubject || want.MaxAge != got.MaxAge || want.DenyPurge != got.DenyPurge || want.DenyDelete != got.DenyDelete || want.NoAck != got.NoAck || want.AllowRollup != got.AllowRollup || want.AllowMsgSchedules != got.AllowMsgSchedules || (want.Duplicates != 0 && want.Duplicates != got.Duplicates) {
		return fmt.Errorf("stream %s configuration mismatch: got %+v", want.Name, got)
	}
	return nil
}
