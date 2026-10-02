package identity

import (
	"errors"
	"fmt"
	"hash/fnv"
)

var ErrInvalidIdentity = errors.New("invalid workflow identity")

// Validate restricts identifiers to one safe NATS subject token.
func Validate(typ, id string) error {
	for _, token := range []string{typ, id} {
		if err := ValidateToken(token); err != nil {
			return err
		}
	}
	return nil
}

func ValidateToken(token string) error {
	if token == "" {
		return fmt.Errorf("%w: %q", ErrInvalidIdentity, token)
	}
	for i := 0; i < len(token); i++ {
		b := token[i]
		if b >= 'a' && b <= 'z' || b >= 'A' && b <= 'Z' || b >= '0' && b <= '9' || b == '_' || b == '-' {
			continue
		}
		return fmt.Errorf("%w: %q", ErrInvalidIdentity, token)
	}
	return nil
}

func Key(typ, id string) string               { return typ + "." + id }
func InvocationSubject(typ, id string) string { return "wf.inv." + Key(typ, id) }
func JournalSubject(typ, id string) string    { return "wf.jrn." + Key(typ, id) }

func Partition(typ, id string, count uint32) uint32 {
	h := fnv.New32a()
	_, _ = h.Write([]byte(Key(typ, id)))
	return h.Sum32() % count
}

func RunSubject(typ, id string, count uint32) string {
	return fmt.Sprintf("wf.run.%d", Partition(typ, id, count))
}

const TimerInvSeqHeader = "Wf-Timer-Inv-Seq"
const TimerStepHeader = "Wf-Timer-Step"
const TimerClockDomainHeader = "Wf-Timer-Clock-Domain"
const TimerDeadlineHeader = "Wf-Timer-Deadline"

func TimerSubject(typ, id string, invSeq, step uint64) string {
	return fmt.Sprintf("wf.timer.%s.%s.%d.%d", typ, id, invSeq, step)
}
