//go:build linux

package integrity

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"js-wf/testcluster"
)

// Controlled loss of a real SDK watch connection, not a model or a relay of
// fabricated entries. The first native watch is held before exposure until its
// connection closes or its actual peer is confirmed dead and reconnecting.
// Its finite SDK buffer cannot contain the full cohort.
// This deliberately exercises partial/closed-watch retry, not the cause of the
// historical silent subscription or a naturally occurring lost reply.
type copiedWatchConnectionLoss struct {
	mu                                             sync.Mutex
	ownerReady, watchReady, exposeReady, restarted chan struct{}
	owner                                          int
	peerOutage                                     bool
	first                                          *nats.Conn
	attempts                                       int
	proof                                          copiedWatchConnectionProof
}

type copiedWatchConnectionProof struct {
	PeerOutage             bool                       `json:"peer_outage,omitempty"`
	Disconnected           time.Time                  `json:"disconnected,omitempty"`
	DisconnectError        string                     `json:"disconnect_error,omitempty"`
	ExposedStatus          string                     `json:"exposed_status,omitempty"`
	RestartStarted         time.Time                  `json:"restart_started,omitempty"`
	OfflineHoldNS          int64                      `json:"offline_hold_ns,omitempty"`
	Owner                  int                        `json:"owner"`
	URL                    string                     `json:"url"`
	ServerID               string                     `json:"server_id"`
	ServerName             string                     `json:"server_name"`
	NativeCreated          time.Time                  `json:"native_created"`
	CloseStarted           time.Time                  `json:"close_started"`
	CloseFinished          time.Time                  `json:"close_finished"`
	StatusAfterClose       string                     `json:"status_after_close"`
	BufferedBeforeExposure int                        `json:"buffered_before_exposure"`
	Exposed                time.Time                  `json:"exposed"`
	Attempts               int                        `json:"attempts"`
	Frames                 []StateSnapshotObservation `json:"frames"`
}

func newCopiedWatchConnectionLoss() *copiedWatchConnectionLoss {
	return &copiedWatchConnectionLoss{ownerReady: make(chan struct{}), watchReady: make(chan struct{}), exposeReady: make(chan struct{}), restarted: make(chan struct{})}
}

func (f *copiedWatchConnectionLoss) observe(frame StateSnapshotObservation) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.proof.Frames = append(f.proof.Frames, frame)
}
func (f *copiedWatchConnectionLoss) snapshot() copiedWatchConnectionProof {
	f.mu.Lock()
	defer f.mu.Unlock()
	result := f.proof
	result.Attempts = f.attempts
	result.Frames = append([]StateSnapshotObservation(nil), result.Frames...)
	return result
}
func (f *copiedWatchConnectionLoss) waitFirst(ctx context.Context) error {
	select {
	case <-f.watchReady:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Expose the actual native Updates channel only after the direct peer is
// confirmed dead and this connection has actually entered reconnecting state.
// No client Close occurs here; the candidate's timeout/Stop owns that cleanup.
func (f *copiedWatchConnectionLoss) exposeDisconnected(ctx context.Context) error {
	for {
		f.mu.Lock()
		nc := f.first
		observed := !f.proof.Disconnected.IsZero()
		f.mu.Unlock()
		if nc != nil && observed && nc.Status() == nats.RECONNECTING {
			f.mu.Lock()
			f.proof.ExposedStatus = nc.Status().String()
			f.mu.Unlock()
			close(f.exposeReady)
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(10 * time.Millisecond):
		}
	}
}

func (f *copiedWatchConnectionLoss) closeFirst(ctx context.Context) error {
	select {
	case <-f.watchReady:
	case <-ctx.Done():
		return ctx.Err()
	}
	f.mu.Lock()
	nc := f.first
	f.proof.CloseStarted = time.Now().UTC()
	f.mu.Unlock()
	if nc == nil {
		return fmt.Errorf("first real watch connection missing")
	}
	nc.Close()
	f.mu.Lock()
	f.proof.CloseFinished = time.Now().UTC()
	f.proof.StatusAfterClose = nc.Status().String()
	f.mu.Unlock()
	close(f.exposeReady)
	return nil
}

type copiedWatchFaultJS struct {
	jetstream.JetStream
	fault   *copiedWatchConnectionLoss
	cluster *testcluster.DockerCluster
}

func (s copiedWatchFaultJS) KeyValue(ctx context.Context, bucket string) (jetstream.KeyValue, error) {
	kv, err := s.JetStream.KeyValue(ctx, bucket)
	if err != nil || bucket != "WF_STATE" {
		return kv, err
	}
	return copiedWatchFaultKV{KeyValue: kv, fault: s.fault, cluster: s.cluster}, nil
}

type copiedWatchFaultKV struct {
	jetstream.KeyValue
	fault   *copiedWatchConnectionLoss
	cluster *testcluster.DockerCluster
}

func (s copiedWatchFaultKV) WatchAll(ctx context.Context, opts ...jetstream.WatchOpt) (jetstream.KeyWatcher, error) {
	f := s.fault
	f.mu.Lock()
	f.attempts++
	attempt := f.attempts
	f.mu.Unlock()
	if attempt != 1 {
		// No sleep or deadline extension: wait for the actual original-store restart
		// before a fresh attempt on the normal multi-peer SDK connection.
		select {
		case <-f.restarted:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
		return s.KeyValue.WatchAll(ctx, opts...)
	}
	select {
	case <-f.ownerReady:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	optsConnect := []nats.Option{nats.IgnoreDiscoveredServers(), nats.Timeout(time.Second), nats.NoReconnect()}
	if f.peerOutage {
		optsConnect = []nats.Option{nats.IgnoreDiscoveredServers(), nats.Timeout(time.Second), nats.MaxReconnects(-1), nats.ReconnectWait(100 * time.Millisecond), nats.DisconnectErrHandler(func(_ *nats.Conn, err error) {
			f.mu.Lock()
			defer f.mu.Unlock()
			if f.proof.Disconnected.IsZero() {
				f.proof.Disconnected = time.Now().UTC()
				f.proof.DisconnectError = fmt.Sprint(err)
			}
		})}
	}
	nc, err := nats.Connect(s.cluster.ClientURL(f.owner), optsConnect...)
	if err != nil {
		return nil, err
	}
	js, err := jetstream.New(nc)
	if err != nil {
		nc.Close()
		return nil, err
	}
	kv, err := js.KeyValue(ctx, "WF_STATE")
	if err != nil {
		nc.Close()
		return nil, err
	}
	watch, err := kv.WatchAll(ctx, opts...)
	if err != nil {
		nc.Close()
		return nil, err
	}
	f.mu.Lock()
	f.first = nc
	f.proof.Owner = f.owner
	f.proof.PeerOutage = f.peerOutage
	f.proof.URL = nc.ConnectedUrl()
	f.proof.ServerID = nc.ConnectedServerId()
	f.proof.ServerName = nc.ConnectedServerName()
	f.proof.NativeCreated = time.Now().UTC()
	f.mu.Unlock()
	close(f.watchReady)
	select {
	case <-f.exposeReady:
		f.mu.Lock()
		f.proof.BufferedBeforeExposure = len(watch.Updates())
		f.proof.Exposed = time.Now().UTC()
		f.mu.Unlock()
		if f.peerOutage {
			return copiedPeerOutageWatch{KeyWatcher: watch, fault: f}, nil
		}
		return watch, nil
	case <-ctx.Done():
		nc.Close()
		_ = watch.Stop()
		// Release any blocked SDK callback after unsubscribing.
		for {
			select {
			case _, ok := <-watch.Updates():
				if !ok {
					return nil, ctx.Err()
				}
			default:
				return nil, ctx.Err()
			}
		}
	}
}

// Closing the dedicated client after the runtime stops its watch prevents its
// automatic reconnect loop from escaping the failed snapshot attempt.
type copiedPeerOutageWatch struct {
	jetstream.KeyWatcher
	fault *copiedWatchConnectionLoss
}

func (s copiedPeerOutageWatch) Stop() error {
	err := s.KeyWatcher.Stop()
	f := s.fault
	f.mu.Lock()
	f.proof.CloseStarted = time.Now().UTC()
	nc := f.first
	f.mu.Unlock()
	nc.Close()
	f.mu.Lock()
	f.proof.CloseFinished = time.Now().UTC()
	f.proof.StatusAfterClose = nc.Status().String()
	f.mu.Unlock()
	return err
}
