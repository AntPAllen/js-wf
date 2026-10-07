package testcluster

import (
	"bufio"
	"bytes"
	"encoding/json"
	"io"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestClientProxyFileTraceJoinedBytesAndFailures(t *testing.T) {
	for _, mode := range []string{"complete", "budget", "write_error"} {
		t.Run(mode, func(t *testing.T) {
			listener, err := net.Listen("tcp", "127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}
			defer listener.Close()
			proxy, err := NewClientProxy("nats://" + listener.Addr().String())
			if err != nil {
				t.Fatal(err)
			}
			defer proxy.Close()
			path := filepath.Join(t.TempDir(), "traffic.frames.jsonl")
			budget := 8 << 20
			if mode == "budget" {
				budget = 1024
			}
			if err = proxy.EnableTrafficFileTrace(path, budget); err != nil {
				t.Fatal(err)
			}
			if err = proxy.EnableTrafficTrace(1 << 20); err == nil {
				t.Fatal("replaced configured trace")
			}
			client, err := net.Dial("tcp", strings.TrimPrefix(proxy.URL(), "nats://"))
			if err != nil {
				t.Fatal(err)
			}
			defer client.Close()
			server, err := listener.Accept()
			if err != nil {
				t.Fatal(err)
			}
			defer server.Close()
			client.SetDeadline(time.Now().Add(5 * time.Second))
			server.SetDeadline(time.Now().Add(5 * time.Second))
			if mode == "write_error" {
				proxy.mu.Lock()
				err = proxy.fileTrace.file.Close()
				proxy.mu.Unlock()
				if err != nil {
					t.Fatal(err)
				}
			}
			payload := bytes.Repeat([]byte("unaltered outbound payload\x00"), 10000)
			sent := make(chan error, 1)
			go func() { _, e := client.Write(payload); sent <- e }()
			got := make([]byte, len(payload))
			if _, err = io.ReadFull(server, got); err != nil || !bytes.Equal(got, payload) {
				t.Fatalf("outbound changed %v", err)
			}
			if err = <-sent; err != nil {
				t.Fatal(err)
			}
			reply := bytes.Repeat([]byte("unaltered response\x00"), 10000)
			go func() { _, e := server.Write(reply); sent <- e }()
			got = make([]byte, len(reply))
			if _, err = io.ReadFull(client, got); err != nil || !bytes.Equal(got, reply) {
				t.Fatalf("inbound changed %v", err)
			}
			if err = <-sent; err != nil {
				t.Fatal(err)
			}
			waitProxyStats(t, proxy, func(s ClientProxyStats) bool {
				return s.ClientToServer == uint64(len(payload)) && s.ServerToClient == uint64(len(reply))
			})
			proxy.Close()
			proxy.Close()
			trace := proxy.TrafficTrace()
			stats := proxy.Stats()
			if len(trace.Frames) != 0 || trace.FrameFile != "traffic.frames.jsonl" || stats.Active != 0 || stats.BufferedBytes != 0 {
				t.Fatalf("trace=%+v stats=%+v", trace, stats)
			}
			if mode != "complete" {
				if !trace.Truncated || trace.FrameRecords != 0 || (mode == "write_error" && trace.FrameFileError == "") {
					t.Fatalf("failure not explicit %+v", trace)
				}
				return
			}
			if trace.Truncated || trace.FrameFileError != "" || trace.FrameRecords == 0 {
				t.Fatalf("unexpected trace failure %+v", trace)
			}
			file, err := os.Open(path)
			if err != nil {
				t.Fatal(err)
			}
			defer file.Close()
			reader := bufio.NewScanner(file)
			reader.Buffer(make([]byte, 64<<10), 1<<20)
			streams := map[string][]byte{}
			var records uint64
			for reader.Scan() {
				var frame ClientProxyFrame
				if err = json.Unmarshal(reader.Bytes(), &frame); err != nil {
					t.Fatal(err)
				}
				if frame.Connection != trace.Connections[0].ID || (frame.Direction != "client_to_server" && frame.Direction != "server_to_client") {
					t.Fatalf("wrong frame %+v", frame)
				}
				streams[frame.Direction] = append(streams[frame.Direction], frame.Data...)
				records++
			}
			if reader.Err() != nil || records != trace.FrameRecords || !bytes.Equal(streams["client_to_server"], payload) || !bytes.Equal(streams["server_to_client"], reply) {
				t.Fatal("file does not bind every forwarded byte")
			}
			if err = proxy.EnableTrafficFileTrace(filepath.Join(t.TempDir(), "new"), 1024); err == nil {
				t.Fatal("configured closed proxy")
			}
		})
	}
}
