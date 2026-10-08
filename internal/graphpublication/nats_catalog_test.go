package graphpublication

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"sort"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

func TestNativeGraphDestinationCatalogEmptyPins(t *testing.T) {
	for _, replicas := range []int{1, 3} {
		t.Run(fmt.Sprintf("R%d", replicas), func(t *testing.T) {
			port, c := nativeGraphObjectFixture(t, replicas)
			p := Protocol{Port: port}
			now := time.Now().UTC()
			want := []string{"absence", "live"}
			if _, err := port.ReadRoot(c, "absence"); err != nil {
				t.Fatal(err)
			}
			for i := 0; i < 12; i++ {
				destination := fmt.Sprintf("empty-%02d", i)
				want = append(want, destination)
				head := uint64(0)
				for pin := 0; pin < MaxReaders; pin++ {
					_, root, err := p.AcquireReader(c, destination, head, now.Add(time.Hour))
					if err != nil {
						t.Fatal(err)
					}
					head = root.Head
				}
			}
			prepared, err := p.PrepareAppend(c, "live", 0, []byte("catalog-retained"), [][]byte{[]byte("catalog-payload")}, now.Add(time.Hour))
			if err != nil {
				t.Fatal(err)
			}
			root, err := p.Commit(c, prepared)
			if err != nil {
				t.Fatal(err)
			}
			reader, root, err := p.AcquireReader(c, "live", root.Head, now.Add(3*time.Hour))
			if err != nil {
				t.Fatal(err)
			}
			if err = p.RetireLive(c, "live", root.Head); err != nil {
				t.Fatal(err)
			}
			sort.Strings(want)
			before, err := port.stream.GetLastMsgForSubject(c, port.subject("root", "live"))
			if err != nil {
				t.Fatal(err)
			}
			keys, err := port.RootKeys(c)
			if err != nil || !reflect.DeepEqual(keys, want) {
				t.Fatal(keys, err)
			}
			after, err := port.stream.GetLastMsgForSubject(c, port.subject("root", "live"))
			if err != nil || before.Sequence != after.Sequence {
				t.Fatal("catalog rewrote authority", err)
			}
			if _, err = p.SweepWithReaders(c, now.Add(time.Hour)); err != nil {
				t.Fatal(err)
			}
			for _, destination := range want {
				current, err := port.ReadRoot(c, destination)
				if err != nil {
					t.Fatal(err)
				}
				if destination == "absence" {
					if current.Head != 0 {
						t.Fatal(current)
					}
					continue
				}
				if destination == "live" {
					if len(current.Readers) != 1 {
						t.Fatal(current)
					}
					continue
				}
				if current.Head != MaxReaders+1 || len(current.Readers) != 0 || current.Schema != RetentionSchema {
					t.Fatal(current)
				}
			}
			got, err := p.ReadRetained(c, reader, 0, now.Add(time.Hour))
			if err != nil || string(got.Data) != "catalog-retained" {
				t.Fatal(got, err)
			}
			if _, err = p.SweepWithReaders(c, now.Add(3*time.Hour)); err != nil {
				t.Fatal(err)
			}
			nativeGraphNoObjects(t, port, c)
			keys, err = port.RootKeys(c)
			if err != nil || !reflect.DeepEqual(keys, want) {
				t.Fatal("catalog lost high-water destinations", keys, err)
			}
		})
	}
}

type catalogFaultStream struct {
	jetstream.Stream
	info    func(*jetstream.StreamInfo)
	message func(*jetstream.RawStreamMsg) error
}

func (s catalogFaultStream) Info(c context.Context, o ...jetstream.StreamInfoOpt) (*jetstream.StreamInfo, error) {
	info, err := s.Stream.Info(c, o...)
	if err != nil {
		return nil, err
	}
	copy := *info
	copy.State.Subjects = map[string]uint64{}
	for k, v := range info.State.Subjects {
		copy.State.Subjects[k] = v
	}
	if s.info != nil {
		s.info(&copy)
	}
	return &copy, nil
}
func (s catalogFaultStream) GetLastMsgForSubject(c context.Context, subject string) (*jetstream.RawStreamMsg, error) {
	message, err := s.Stream.GetLastMsgForSubject(c, subject)
	if err != nil {
		return nil, err
	}
	copy := *message
	copy.Data = append([]byte(nil), message.Data...)
	if s.message != nil {
		if err = s.message(&copy); err != nil {
			return nil, err
		}
	}
	return &copy, nil
}

func TestNativeGraphDestinationCatalogRejectsPartialAndCorrupt(t *testing.T) {
	port, c := nativeGraphObjectFixture(t, 1)
	p := Protocol{Port: port}
	now := time.Now().UTC()
	_, root, err := p.AcquireReader(c, "empty", 0, now.Add(time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	original := port.stream
	for _, mode := range []string{"partial", "count", "namespace", "root-subject", "unknown-get", "wrong-subject", "zero-sequence", "invalid-json", "foreign-identity", "unknown-schema", "oversized"} {
		t.Run(mode, func(t *testing.T) {
			fault := catalogFaultStream{Stream: original}
			fault.info = func(info *jetstream.StreamInfo) {
				switch mode {
				case "partial":
					info.State.NumSubjects++
				case "count":
					for key := range info.State.Subjects {
						info.State.Subjects[key] = 2
					}
				case "namespace":
					info.State.Subjects[port.prefix+".foreign.bad"] = 1
					info.State.NumSubjects++
				case "root-subject":
					info.State.Subjects[port.prefix+".root.bad"] = 1
					info.State.NumSubjects++
				}
			}
			fault.message = func(message *jetstream.RawStreamMsg) error {
				switch mode {
				case "unknown-get":
					return lostReply
				case "wrong-subject":
					message.Subject += "changed"
				case "zero-sequence":
					message.Sequence = 0
				case "invalid-json":
					message.Data = []byte("{")
				case "oversized":
					message.Data = make([]byte, MaxAuthorityBytes+1)
				case "foreign-identity", "unknown-schema":
					var v authorityValue
					if err := json.Unmarshal(message.Data, &v); err != nil {
						return err
					}
					if mode == "foreign-identity" {
						v.Identity = "other"
					} else {
						v.Schema = "legacy"
					}
					message.Data, _ = json.Marshal(v)
				}
				return nil
			}
			port.stream = fault
			keys, err := port.RootKeys(c)
			if err == nil || keys != nil {
				t.Fatal("partial catalog accepted", keys, err)
			}
			if _, err = p.SweepWithReaders(c, now.Add(time.Hour)); err == nil {
				t.Fatal("invalid catalog allowed expiry")
			}
			port.stream = original
			current, err := port.ReadRoot(c, "empty")
			if err != nil || current.Head != root.Head || len(current.Readers) != 1 {
				t.Fatal(current, err)
			}
		})
	}
	cancelled, cancel := context.WithCancel(c)
	cancel()
	if keys, err := port.RootKeys(cancelled); !errors.Is(err, context.Canceled) || keys != nil {
		t.Fatal(keys, err)
	}
}
