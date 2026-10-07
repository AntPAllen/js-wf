package main

import (
	"context"
	"errors"
	"net"
	"sync"
	"sync/atomic"

	"github.com/nats-io/nats.go"
)

// Closing the socket interrupts INFO, TLS, CONNECT and PONG reads as well as
// dialing. No background Connect goroutine can outlive a canceled startup.
func connectDaemon(ctx context.Context, endpoint string) (*nats.Conn, error) {
	dialer := &daemonDialer{ctx: ctx, armed: true}
	nc, err := nats.Connect(endpoint, nats.SetCustomDialer(dialer), nats.SkipHostLookup())
	// Established connections retain the existing daemon shutdown behavior:
	// cancel API work first, then close the NATS client on return.
	dialer.disarm()
	if err == nil && ctx.Err() != nil {
		nc.Close()
		return nil, ctx.Err()
	}
	return nc, err
}

type daemonDialer struct {
	ctx   context.Context
	mu    sync.Mutex
	armed bool
	stops []func() bool
}

func (d *daemonDialer) disarm() {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.armed = false
	for _, stop := range d.stops {
		stop()
	}
	d.stops = nil
}

func (d *daemonDialer) Dial(network, address string) (net.Conn, error) {
	dialer := net.Dialer{Timeout: nats.DefaultTimeout}
	conn, err := dialer.DialContext(d.ctx, network, address)
	if err != nil {
		return nil, err
	}
	wrapped := &daemonConnection{Conn: conn, ctx: d.ctx}
	d.mu.Lock()
	if d.armed {
		wrapped.stop = context.AfterFunc(d.ctx, func() {
			wrapped.canceled.Store(true)
			_ = conn.Close()
		})
		d.stops = append(d.stops, wrapped.stop)
	}
	d.mu.Unlock()
	return wrapped, nil
}

type daemonConnection struct {
	net.Conn
	ctx      context.Context
	stop     func() bool
	canceled atomic.Bool
}

func (c *daemonConnection) Read(b []byte) (int, error) {
	n, err := c.Conn.Read(b)
	return n, c.cancellationError(err)
}

func (c *daemonConnection) Write(b []byte) (int, error) {
	n, err := c.Conn.Write(b)
	return n, c.cancellationError(err)
}

func (c *daemonConnection) cancellationError(err error) error {
	// Only a locally closed socket caused by cancellation is a clean shutdown.
	// Remote EOF, authentication rejection and protocol errors remain failures.
	if errors.Is(err, net.ErrClosed) && c.canceled.Load() && c.ctx.Err() != nil {
		return c.ctx.Err()
	}
	return err
}

func (c *daemonConnection) Close() error {
	if c.stop != nil {
		c.stop()
	}
	return c.Conn.Close()
}
