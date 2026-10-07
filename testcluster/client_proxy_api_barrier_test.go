package testcluster

import (
	"bufio"
	"bytes"
	"io"
	"net"
	"strconv"
	"strings"
	"testing"
	"time"
)

func TestClientProxyFirstAPIBarrier(t *testing.T) {
	for _, action := range []string{"release", "close", "block", "upstream_eof"} {
		t.Run(action, func(t *testing.T) {
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
			if err = proxy.EnableTrafficTrace(1 << 20); err != nil {
				t.Fatal(err)
			}
			if err = proxy.HoldFirstAPI("$JS.WFOPS.API."); err != nil {
				t.Fatal(err)
			}
			client, err := net.Dial("tcp", proxy.listener.Addr().String())
			if err != nil {
				t.Fatal(err)
			}
			defer client.Close()
			server, err := listener.Accept()
			if err != nil {
				t.Fatal(err)
			}
			defer server.Close()
			client.SetDeadline(time.Now().Add(3 * time.Second))
			server.SetDeadline(time.Now().Add(3 * time.Second))
			// API-looking payload bytes must not be interpreted as a publication header.
			payload := "PUB $JS.WFOPS.API.FAKE reply 0\r\n\r\n"
			prefix := "PING\r\nPUB ordinary " + strconv.Itoa(len(payload)) + "\r\n" + payload + "\r\n"
			packet := "HPUB $JS.WFOPS.API.INFO reply 12 14\r\nNATS/1.0\r\n\r\n{}\r\n"
			// Fragment through every possible header/payload boundary.
			for _, b := range []byte(prefix + packet) {
				if _, err = client.Write([]byte{b}); err != nil {
					t.Fatal(err)
				}
			}
			got := make([]byte, len(prefix))
			if _, err = io.ReadFull(server, got); err != nil || string(got) != prefix {
				t.Fatalf("prefix %q %v", got, err)
			}
			until := time.Now().Add(3 * time.Second)
			for proxy.PendingAPI() == nil && time.Now().Before(until) {
				time.Sleep(time.Millisecond)
			}
			pending := proxy.PendingAPI()
			if pending == nil || pending.Subject != "$JS.WFOPS.API.INFO" || string(pending.Packet) != packet || pending.Disposition != "held" || pending.ForwardedBytes != 0 {
				t.Fatalf("pending %+v", pending)
			}
			pending.Packet[0] = 'X'
			if proxy.PendingAPI().Packet[0] != 'H' {
				t.Fatal("snapshot aliases packet")
			}
			if err = proxy.HoldFirstAPI("$JS.WFOPS.API."); err == nil {
				t.Fatal("reconfigured used proxy")
			}
			server.SetReadDeadline(time.Now().Add(20 * time.Millisecond))
			if n, _ := server.Read(make([]byte, 1)); n != 0 {
				t.Fatal("held packet escaped")
			}
			server.SetReadDeadline(time.Now().Add(3 * time.Second))
			switch action {
			case "release":
				proxy.ReleaseFirstAPI()
				proxy.ReleaseFirstAPI()
				next := "PUB $JS.WFOPS.API.INFO reply 0\r\n\r\n"
				if _, err = client.Write([]byte(next)); err != nil {
					t.Fatal(err)
				}
				got = make([]byte, len(packet+next))
				if _, err = io.ReadFull(server, got); err != nil || string(got) != packet+next {
					t.Fatalf("release %q %v", got, err)
				}
			case "block":
				proxy.Block()
				waitProxyStats(t, proxy, func(s ClientProxyStats) bool { return s.Active == 0 })
			case "upstream_eof":
				server.Close()
				waitProxyStats(t, proxy, func(s ClientProxyStats) bool { return s.Active == 0 })
			}
			joined := make(chan struct{})
			go func() { proxy.Close(); close(joined) }()
			select {
			case <-joined:
			case <-time.After(3 * time.Second):
				t.Fatal("Close did not join held relay")
			}
			pending = proxy.PendingAPI()
			want := uint64(len(prefix))
			disposition := "cancelled"
			if action == "release" {
				want += uint64(len(packet) + len("PUB $JS.WFOPS.API.INFO reply 0\r\n\r\n"))
				disposition = "forwarded"
			}
			if proxy.Stats().ClientToServer != want || pending.Disposition != disposition || proxy.Stats().Active != 0 {
				t.Fatalf("final pending=%+v stats=%+v", pending, proxy.Stats())
			}
			var wire []byte
			for _, frame := range proxy.TrafficTrace().Frames {
				if frame.Direction == "client_to_server" {
					wire = append(wire, frame.Data...)
				}
			}
			if uint64(len(wire)) != want || !bytes.HasPrefix(wire, []byte(prefix)) {
				t.Fatalf("forwarded transcript len=%d want=%d", len(wire), want)
			}
			if action != "release" && pending.ForwardedBytes != 0 {
				t.Fatal("cancelled held publication had forwarded bytes")
			}
		})
	}
}

func TestReadClientPacketRejectsMalformedPublications(t *testing.T) {
	for _, input := range []string{
		"PUB subject -1\r\n", "PUB subject 16777217\r\n", "PUB subject nope\r\n",
		"PUB subject 1\r\nxXX", "HPUB subject 4 2\r\n", "HPUB subject 1 nope\r\n",
		"PUB\r\n", "PING\n", strings.Repeat("X", 64<<10) + "\r\n",
		"PUB subject 2\r\nx", "HPUB subject -1 2\r\n",
	} {
		if _, _, err := readClientPacket(bufio.NewReaderSize(strings.NewReader(input), 64<<10)); err == nil {
			t.Fatalf("accepted malformed input %q", input[:min(len(input), 80)])
		}
	}
}
