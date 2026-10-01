package natsutil

import (
	"errors"
	"fmt"
	"github.com/nats-io/nats.go/jetstream"
	"testing"
)

func TestUnavailableReplyForms(t *testing.T) {
	for _, test := range []struct {
		err  error
		want bool
	}{
		{&jetstream.APIError{Code: 503, ErrorCode: 10008}, true},
		{errors.New("nats: JetStream system temporarily unavailable"), true},
		{fmt.Errorf("retained read: %w", errors.New("nats: JetStream system temporarily unavailable")), true},
		{&jetstream.APIError{Code: 503, ErrorCode: 10158}, false},
		{errors.New("invalid journal: nats: JetStream system temporarily unavailable"), false},
		{errors.New("nats: unable to get message"), false},
		{jetstream.ErrMsgNotFound, false}, {nil, false},
	} {
		if got := IsUnavailable(test.err); got != test.want {
			t.Fatalf("err=%v got=%v want=%v", test.err, got, test.want)
		}
	}
}
