#!/usr/bin/env python3
"""Pinned snapshot-abort fixture controls for a permit returned after draining."""
import argparse
import hashlib
import json
from pathlib import Path

NAME = 'TestNRGCheckpointInstallSnapshotAbortDuringWrite'
DRAIN = '''\t\t\tdrained := 0
\t\tdrain:
\t\t\tfor {
\t\t\t\tselect {
\t\t\t\tcase <-n.dios.ch:
\t\t\t\t\tdrained++
\t\t\t\tdefault:
\t\t\t\t\tbreak drain
\t\t\t\t}
\t\t\t}
'''
RESERVE = '''\t\t\tdrained := 0
\t\t\tfor drained < n.dios.cap() {
\t\t\t\tn.dios.acquire()
\t\t\t\tdrained++
\t\t\t}
'''
INJECT = '''\t\t\t// Model one real in-flight I/O holder returning after a waiter blocks.
\t\t\tn.dios.acquire()
\t\t\tlateReturned := make(chan struct{})
\t\t\tlateWaiterObserved := make(chan struct{})
\t\t\tgo func() {
\t\t\t\tdefer close(lateReturned)
\t\t\t\tdeadline := time.NewTimer(5 * time.Second)
\t\t\t\tdefer deadline.Stop()
\t\t\t\tfor n.dios.waiters.Load() == 0 {
\t\t\t\t\tselect {
\t\t\t\t\tcase <-deadline.C:
\t\t\t\t\t\tn.dios.release()
\t\t\t\t\t\treturn
\t\t\t\t\tdefault:
\t\t\t\t\t\ttime.Sleep(time.Millisecond)
\t\t\t\t\t}
\t\t\t\t}
\t\t\t\tclose(lateWaiterObserved)
\t\t\t\tn.dios.release()
\t\t\t}()
\t\t\tdefer func() { <-lateReturned }()

'''
OBSERVE = '''\t\t\tselect {
\t\t\tcase <-lateWaiterObserved:
\t\t\tdefault:
\t\t\t\tt.Fatal("late-return control did not observe a semaphore waiter")
\t\t\t}
\t\t\tt.Logf("DIAGNOSTIC_LATE_DIOS borrowed=1 waited=true drained=%d capacity=%d", drained, n.dios.cap())
'''


def transform(original, reserve_all=False, late_return=False):
    start = original.index('func ' + NAME + '(')
    end = original.index('\nfunc ', start + 1)
    body = original[start:end]
    if body.count(DRAIN) != 1 or body.count('\t\t\ttime.Sleep(100 * time.Millisecond)\n') != 1:
        raise ValueError('unexpected pinned snapshot fixture shape')
    changed = body.replace(DRAIN, RESERVE) if reserve_all else body
    if late_return:
        anchor = '\t\t\t// Drain dios so writeFileWithSync parks inside writeAtomically.\n'
        if changed.count(anchor) != 1:
            raise ValueError('missing pinned drain comment')
        changed = changed.replace(anchor, INJECT + anchor)
        changed = changed.replace('\t\t\ttime.Sleep(100 * time.Millisecond)\n', '\t\t\ttime.Sleep(100 * time.Millisecond)\n' + OBSERVE)
    reversed_body = changed
    if late_return:
        reversed_body = reversed_body.replace(INJECT, '').replace(OBSERVE, '')
    if reserve_all:
        reversed_body = reversed_body.replace(RESERVE, DRAIN)
    if reversed_body != body:
        raise ValueError('change exceeds pinned permit reservation and explicit diagnostic injection')
    return original[:start] + changed + original[end:]


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--original', type=Path, required=True)
    p.add_argument('--output', type=Path, required=True)
    p.add_argument('--reserve-all', action='store_true')
    p.add_argument('--late-return', action='store_true')
    a = p.parse_args()
    if a.output.exists() or a.output.resolve() == a.original.resolve():
        p.error('require fresh separate output')
    original = a.original.read_text()
    modified = transform(original, a.reserve_all, a.late_return)
    a.output.write_text(modified)
    print(json.dumps(dict(test=NAME, reserve_all=a.reserve_all, late_return=a.late_return,
                          original_sha256=hashlib.sha256(original.encode()).hexdigest(),
                          modified_sha256=hashlib.sha256(modified.encode()).hexdigest(),
                          scope='Only the selected snapshot test drain loop and optional explicit late-permit control. Every original assertion, subcase, sleep and timeout remains. No production change or historical-cause claim.')))


if __name__ == '__main__':
    main()
