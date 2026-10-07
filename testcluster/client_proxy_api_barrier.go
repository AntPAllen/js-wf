package testcluster

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
)

// ClientProxyPendingAPI is separate from TrafficTrace: its packet was received
// from the client but is not forwarded while held. It proves a controlled
// pre-publication startup boundary, not a committed request or lost response.
type ClientProxyPendingAPI struct {
	Connection     uint64 `json:"connection"`
	Subject        string `json:"subject"`
	Packet         []byte `json:"packet"`
	Disposition    string `json:"disposition"`
	ForwardedBytes uint64 `json:"forwarded_bytes"`
}

type clientAPIBarrier struct {
	prefix   string
	release  chan struct{}
	cancel   chan struct{}
	released bool
	pending  *ClientProxyPendingAPI
}

// HoldFirstAPI must be configured before connecting. Only this opt-in fixture
// path parses client packets; ordinary relay behavior is unchanged. The first
// complete PUB/HPUB to prefix is held before writing any of its bytes upstream.
func (p *ClientProxy) HoldFirstAPI(prefix string) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed || p.acceptedConnections != 0 || p.apiBarrier != nil ||
		!strings.HasPrefix(prefix, "$JS.") || !strings.HasSuffix(prefix, ".API.") || strings.ContainsAny(prefix, " \t\r\n") {
		return fmt.Errorf("API barrier requires an unused proxy and a JetStream API prefix")
	}
	p.apiBarrier = &clientAPIBarrier{prefix: prefix, release: make(chan struct{}), cancel: make(chan struct{})}
	return nil
}

func (p *ClientProxy) PendingAPI() *ClientProxyPendingAPI {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.apiBarrier == nil || p.apiBarrier.pending == nil {
		return nil
	}
	copy := *p.apiBarrier.pending
	copy.Packet = append([]byte(nil), copy.Packet...)
	return &copy
}

func (p *ClientProxy) ReleaseFirstAPI() {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.apiBarrier != nil && !p.apiBarrier.released {
		p.apiBarrier.released = true
		close(p.apiBarrier.release)
	}
}

// Parse whole publications before interpreting subjects, so API-looking payload
// bytes cannot trigger a gate. Header/payload bounds keep malformed fixture
// traffic from creating an unbounded allocation.
func readClientPacket(reader *bufio.Reader) ([]byte, string, error) {
	line, err := reader.ReadSlice('\n')
	if err != nil {
		return nil, "", err
	}
	if !bytes.HasSuffix(line, []byte("\r\n")) {
		return nil, "", fmt.Errorf("invalid NATS control terminator")
	}
	packet := append([]byte(nil), line...)
	fields := strings.Fields(string(line[:len(line)-2]))
	if len(fields) == 0 {
		return nil, "", fmt.Errorf("empty NATS control")
	}
	if fields[0] != "PUB" && fields[0] != "HPUB" {
		return packet, "", nil
	}
	min := 3
	if fields[0] == "HPUB" {
		min = 4
	}
	if len(fields) != min && len(fields) != min+1 {
		return nil, "", fmt.Errorf("invalid publication header")
	}
	size, err := strconv.Atoi(fields[len(fields)-1])
	if err != nil || size < 0 || size > 16<<20 {
		return nil, "", fmt.Errorf("invalid publication payload size")
	}
	if fields[0] == "HPUB" {
		headers, e := strconv.Atoi(fields[len(fields)-2])
		if e != nil || headers < 0 || headers > size {
			return nil, "", fmt.Errorf("invalid header payload size")
		}
	}
	payload := make([]byte, size+2)
	if _, err = io.ReadFull(reader, payload); err != nil {
		return nil, "", err
	}
	if !bytes.HasSuffix(payload, []byte("\r\n")) {
		return nil, "", fmt.Errorf("invalid publication terminator")
	}
	return append(packet, payload...), fields[1], nil
}

func (p *ClientProxy) copyWithAPIBarrier(client, upstream net.Conn, connection uint64, sessionDone <-chan struct{}, barrier *clientAPIBarrier) error {
	reader := bufio.NewReaderSize(client, 64<<10)
	writer := clientProxyWriter{p, upstream, connection}
	for {
		packet, subject, err := readClientPacket(reader)
		if err != nil {
			return err
		}
		p.mu.Lock()
		held := barrier.pending == nil && strings.HasPrefix(subject, barrier.prefix)
		if held {
			barrier.pending = &ClientProxyPendingAPI{Connection: connection, Subject: subject, Packet: append([]byte(nil), packet...), Disposition: "held"}
		}
		p.mu.Unlock()
		if held {
			select {
			case <-barrier.release:
			case <-barrier.cancel:
				p.cancelPendingAPI(barrier)
				return io.EOF
			case <-sessionDone:
				p.cancelPendingAPI(barrier)
				return io.EOF
			}
		}
		for offset := 0; offset < len(packet); {
			n, e := writer.Write(packet[offset:])
			offset += n
			if held {
				p.mu.Lock()
				barrier.pending.ForwardedBytes += uint64(n)
				p.mu.Unlock()
			}
			if e != nil || n == 0 {
				if held {
					p.mu.Lock()
					barrier.pending.Disposition = "write_failed"
					p.mu.Unlock()
				}
				if e != nil {
					return e
				}
				return io.ErrShortWrite
			}
		}
		if held {
			p.mu.Lock()
			barrier.pending.Disposition = "forwarded"
			p.mu.Unlock()
		}
	}
}

func (p *ClientProxy) cancelPendingAPI(barrier *clientAPIBarrier) {
	p.mu.Lock()
	defer p.mu.Unlock()
	barrier.pending.Disposition = "cancelled"
}
