package integrity

import (
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

// compactAckCoordinates recognizes only the pinned SDK's ordinary v1/v2 ACK
// forms. Unsupported syntax is not rejected: callers must use SDK Metadata.
// Slices refer to the immutable reply string; no token slice or metadata object
// is allocated. Every numeric field is checked to keep this path conservative.
func compactAckCoordinates(reply string) (string, uint64, time.Time, bool) {
	var tokens [12]string
	rest := reply
	count := 0
	for {
		if count == len(tokens) {
			return "", 0, time.Time{}, false
		}
		token, tail, more := strings.Cut(rest, ".")
		tokens[count] = token
		count++
		if !more {
			break
		}
		rest = tail
	}
	if count != 9 && count != 11 && count != 12 {
		return "", 0, time.Time{}, false
	}
	if tokens[0] != "$JS" || tokens[1] != "ACK" {
		return "", 0, time.Time{}, false
	}
	streamPos := 2
	if count != 9 {
		streamPos = 4
	}
	if tokens[streamPos] == "" || tokens[streamPos+1] == "" {
		return "", 0, time.Time{}, false
	}
	var sequence, timestamp uint64
	for pos := streamPos + 2; pos <= streamPos+6; pos++ {
		value, err := strconv.ParseUint(tokens[pos], 10, 64)
		if err != nil {
			return "", 0, time.Time{}, false
		}
		if pos == streamPos+3 {
			sequence = value
		}
		if pos == streamPos+5 {
			timestamp = value
		}
	}
	return tokens[streamPos], sequence, time.Unix(0, int64(timestamp)), true
}

// Only the SDK's concrete delivery receiver takes the experimental path.
// Wrappers and fault models retain their Metadata overrides, including errors
// and nil metadata. SDK delivery receivers are bound by the consumer iterator.
func compactMessageCoordinates(msg jetstream.Msg) (string, uint64, time.Time, error) {
	typ := reflect.TypeOf(msg)
	if typ != nil && typ.Kind() == reflect.Pointer && typ.Elem().PkgPath() == "github.com/nats-io/nats.go/jetstream" && typ.Elem().Name() == "jetStreamMsg" && !reflect.ValueOf(msg).IsNil() {
		if stream, seq, timestamp, ok := compactAckCoordinates(msg.Reply()); ok {
			return stream, seq, timestamp, nil
		}
	}
	return sdkMessageCoordinates(msg)
}
