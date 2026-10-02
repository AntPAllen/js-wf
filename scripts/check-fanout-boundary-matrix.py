#!/usr/bin/env python3
"""Require all six executed 500-child parent SIGKILL boundaries and prefix proofs."""
import argparse
import json
from pathlib import Path
import re

TEST = 'TestFiveHundredChildFanoutParentBoundaryMatrix'
CASES = tuple(f'{phase}/{position}' for phase in ('create', 'results') for position in ('first', 'interior', 'last'))


def check(events):
    terminals = [e for e in events if e.get('Action') in ('pass', 'fail', 'skip')]
    for name in (TEST, *(TEST + '/' + case for case in CASES)):
        found = [e for e in terminals if e.get('Test') == name]
        if len(found) != 1 or found[0]['Action'] != 'pass':
            raise ValueError(f'missing or nonpassing boundary {name}')
    package = [e for e in terminals if 'Test' not in e]
    if len(package) != 1 or package[0]['Action'] != 'pass':
        raise ValueError('package completion failed or missing')
    result = []
    for case in CASES:
        phase, position = case.split('/')
        log = ''.join(e.get('Output', '') for e in events if e.get('Test') == TEST + '/' + case)
        kills = re.findall(r'SIGKILLed parent worker at (\w+) cut (\d+); retained entries=(\d+)', log)
        proofs = re.findall(r'FANOUT_PREFIX_PRESERVED phase=(\w+) entries=(\d+) old_epoch=(\d+) final_epoch=(\d+)', log)
        completions = re.findall(r'completed (\d+) children; (\d+) shared the parent partition', log)
        if len(kills) != 1 or len(proofs) != 1 or len(completions) != 1:
            raise ValueError(f'{case}: missing or ambiguous physical cut, prefix or completion proof')
        actual_phase, cut, entries = kills[0]
        preserved_phase, preserved, old_epoch, final_epoch = proofs[0]
        cut, entries, preserved, old_epoch, final_epoch = map(int, (cut, entries, preserved, old_epoch, final_epoch))
        if actual_phase != phase or preserved_phase != phase or entries != preserved or old_epoch < 1 or final_epoch <= old_epoch:
            raise ValueError(f'{case}: changed prefix or unfenced successor')
        if (position == 'first' and cut != 0) or (position == 'last' and cut != 499) or (position == 'interior' and not 100 <= cut < 400):
            raise ValueError(f'{case}: incorrect boundary position')
        if phase == 'create' and entries != 1 + 2 * (cut + 1):
            raise ValueError(f'{case}: incorrect committed creation prefix length')
        if completions[0][0] != '500' or not 0 <= int(completions[0][1]) <= 500:
            raise ValueError(f'{case}: incomplete fanout')
        result.append(dict(phase=phase, position=position, cut=cut, preserved_entries=entries,
                           old_epoch=old_epoch, final_epoch=final_epoch, children=500))
    return dict(cases=result, invocations_per_case=501,
                scope='Six R3 parent process SIGKILL boundaries with 500 children each; not the whole chaos matrix.',
                clears_full_release=False)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--events', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    report = check([json.loads(line) for line in args.events.read_text().splitlines() if line.strip()])
    args.output.write_text(json.dumps(report, indent=2) + '\n')
