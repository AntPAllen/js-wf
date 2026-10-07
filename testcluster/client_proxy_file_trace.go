package testcluster

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"time"
)

type clientProxyFileTrace struct{ file *os.File }

// EnableTrafficFileTrace retains all successfully forwarded bytes as JSONL
// frames in a new file. The budget counts encoded bytes and connection overhead;
// exhaustion or write/close errors permanently invalidate complete-capture
// claims. Configure before connecting; Close joins traffic and closes the file.
// Connections remain in TrafficTrace, while frames are read from FrameFile
// beside the caller's saved trace metadata. Payloads are never retained in RAM.
func (p *ClientProxy) EnableTrafficFileTrace(path string, limit int) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed || p.acceptedConnections != 0 || p.trafficLimit != 0 || limit < 1024 || limit > 1<<30 {
		return fmt.Errorf("file trace requires an unused proxy and a 1 KiB..1 GiB budget")
	}
	file, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		return err
	}
	p.trafficLimit = limit
	p.fileTrace = &clientProxyFileTrace{file}
	p.traffic.FrameFile = filepath.Base(path)
	return nil
}

func (p *ClientProxy) recordFileTrafficLocked(connection uint64, direction string, data []byte) {
	frame := ClientProxyFrame{connection, direction, data, time.Now().UTC()}
	encoded, err := json.Marshal(frame)
	if err != nil {
		p.failFileTraceLocked(err)
		return
	}
	encoded = append(encoded, '\n')
	if p.trafficUsed+len(encoded) > p.trafficLimit {
		p.traffic.Truncated = true
		return
	}
	n, err := p.fileTrace.file.Write(encoded)
	p.trafficUsed += n
	if err != nil || n != len(encoded) {
		if err == nil {
			err = fmt.Errorf("short trace write: %d/%d", n, len(encoded))
		}
		p.failFileTraceLocked(err)
		return
	}
	p.traffic.FrameRecords++
}

func (p *ClientProxy) failFileTraceLocked(err error) {
	p.traffic.Truncated = true
	p.traffic.FrameFileError = err.Error()
}

func (p *ClientProxy) closeFileTrace() {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.fileTrace == nil || p.fileTrace.file == nil {
		return
	}
	if err := p.fileTrace.file.Close(); err != nil {
		p.failFileTraceLocked(err)
	}
	// Retain the mode object: this proxy is closed and cannot accept traffic, and
	// repeated Close must join safely without closing the file twice.
	p.fileTrace.file = nil
}
