#!/usr/bin/env python3
"""Emit an experimental NATS source overlay; never modify the pinned module."""
import argparse
import hashlib
import json
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
OLD = '''	// If we are/were catching up ignore old catchup subs, but only if catching up from an older server
	// that doesn't send the leader term when catching up or if we would truncate as a result.
	// We can reject old catchups from newer subs later, just by checking the append entry is on the correct term.
	if !isNew && sub != nil && (ae.lterm == 0 || ae.pindex < n.pindex) && (!catchingUp || sub != n.catchup.sub) {'''
NEW = '''	// A queued callback from a canceled or superseded catchup subscription
	// cannot establish new catchup state. Its reply is a progress inbox, not
	// the leader's general append response inbox. Reject it even when it
	// carries the current leader term; replay (sub == nil) is unaffected.
	if isCatchup && (!catchingUp || sub != n.catchup.sub) {'''
CONTIGUOUS = '''\t// Obsolete catchup callbacks cannot start or replace catchup state.
\t// After catchup completes, a modern stream can still deliver a trailing
\t// entry. Accept it only from our known current-term leader and only when
\t// its previous term/index exactly match our log: no reset or new catchup
\t// request can then be caused by its obsolete progress reply inbox.
\tobsoleteCatchup := isCatchup && (!catchingUp || sub != n.catchup.sub)
\tcontiguousTrailing := !catchingUp && ae.lterm != 0 && ae.lterm == n.term &&
\t\tae.leader != noLeader && ae.leader == n.leader &&
\t\tae.pterm == n.pterm && ae.pindex == n.pindex
\tif obsoleteCatchup && !contiguousTrailing {'''


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--original', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--variant', choices=['strict', 'contiguous'], default='strict',
                        help='Strict preserves the historical candidate; contiguous is the experimental refinement.')
    args = parser.parse_args()
    assert args.original.resolve() != args.output.resolve() and not args.output.exists()
    data = args.original.read_bytes()
    original = json.loads((REPO/'docs/scale/local-tier2-partition-2026-10-06/seed-006-failure/external-source-before.json').read_text())
    recorded = original['/home/exedev/go/pkg/mod/github.com/nats-io/nats-server/v2@v2.15.0/server/raft.go']
    assert hashlib.sha256(data).hexdigest() == recorded
    text = data.decode()
    assert text.count(OLD) == 1
    args.output.write_text(text.replace(OLD, NEW if args.variant == 'strict' else CONTIGUOUS))


if __name__ == '__main__':
    main()
