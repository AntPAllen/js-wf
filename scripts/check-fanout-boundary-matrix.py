#!/usr/bin/env python3
"""Require all six executed 500-child parent SIGKILL boundaries and prefix proofs."""
import argparse
import json
from pathlib import Path
import re

TEST = 'TestFiveHundredChildFanoutParentBoundaryMatrix'
COMBINED_TEST = 'TestFiveHundredChildFanoutCombinedParentAndJournalBoundaryMatrix'
CASES = tuple(f'{phase}/{position}' for phase in ('create', 'results') for position in ('first', 'interior', 'last'))


def check(events, combined=False, physical_drain=False):
    test = COMBINED_TEST if combined else TEST
    terminals = [e for e in events if e.get('Action') in ('pass', 'fail', 'skip')]
    for name in (test, *(test + '/' + case for case in CASES)):
        found = [e for e in terminals if e.get('Test') == name]
        if len(found) != 1 or found[0]['Action'] != 'pass':
            raise ValueError(f'missing or nonpassing boundary {name}')
    package = [e for e in terminals if 'Test' not in e]
    if len(package) != 1 or package[0]['Action'] != 'pass':
        raise ValueError('package completion failed or missing')
    result = []
    for case in CASES:
        phase, position = case.split('/')
        log = ''.join(e.get('Output', '') for e in events if e.get('Test') == test + '/' + case)
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
        if physical_drain:
            drains = list(re.finditer(r'FANOUT_PHYSICAL_DRAIN peers=3 stream_messages=0 consumers=64 pending=0 ack_pending=0 workers_joined=64(?=\s|$)', log))
            prefix = re.search(r'FANOUT_PREFIX_PRESERVED ', log)
            kill = re.search(r'SIGKILLed parent worker at ', log)
            if len(drains) != 1 or not kill.start() < drains[0].start() < prefix.start():
                raise ValueError(f'{case}: missing, ambiguous or reordered physical drain')
        restart = None
        if combined:
            matches = list(re.finditer(r'combined journal restart phase=(\w+) node=(\d+) messages=(\d+) tail_seq=(\d+) prefix_tail_seq=(\d+)', log))
            if len(matches) != 1:
                raise ValueError(f'{case}: missing or ambiguous combined journal restart')
            match = matches[0]
            actual_phase, node, messages, tail, prefix_tail = match.groups()
            node, messages, tail, prefix_tail = map(int, (node, messages, tail, prefix_tail))
            kill = re.search(r'SIGKILLed parent worker at ', log)
            prefix = re.search(r'FANOUT_PREFIX_PRESERVED ', log)
            if actual_phase != phase or not 0 <= node < 3 or messages < 1 or prefix_tail < entries or tail < prefix_tail or not kill.start() < match.start() < prefix.start():
                raise ValueError(f'{case}: incorrect combined fault ordering/identity/count')
            if physical_drain and match.start() >= drains[0].start():
                raise ValueError(f'{case}: physical drain precedes journal fault')
            if phase == 'create' and (messages != entries or tail != entries or prefix_tail != entries):
                raise ValueError(f'{case}: creation boundary journal count mismatch')
            if phase == 'results':
                outside = list(re.finditer(r'confirmed (\d+) outside child results before worker stop', log))
                if len(outside) != 1 or int(outside[0].group(1)) + int(completions[0][1]) != 500 or outside[0].start() >= kill.start():
                    raise ValueError(f'{case}: outside children not confirmed before worker stop and kill')
                all_children = list(re.finditer(r'confirmed (\d+) child results including (\d+) parent-partition children before worker stop', log))
                prepared = list(re.finditer(r'prepared (\d+) child signals before result cut without parent terminal', log))
                if len(all_children) != 1 or all_children[0].groups() != ('500', completions[0][1]) or len(prepared) != 1 or prepared[0].group(1) != '500' or not outside[0].start() < all_children[0].start() < prepared[0].start() < kill.start():
                    raise ValueError(f'{case}: shared children or signal preparation missing or unordered')
            restart = dict(node=node, retained_journal_messages=messages, journal_tail_sequence=tail, captured_prefix_tail_sequence=prefix_tail)
        result.append(dict(phase=phase, position=position, cut=cut, preserved_entries=entries,
                           old_epoch=old_epoch, final_epoch=final_epoch, children=500,
                           **({"journal_restart": restart} if combined else {})))
    return dict(cases=result, invocations_per_case=501,
                scope=('Six R3 parent SIGKILL + library journal restart boundaries with 500 children each; not server SIGKILL or the whole chaos matrix.' if combined else 'Six R3 parent process SIGKILL boundaries with 500 children each; not the whole chaos matrix.'),
                physical_drain_required=physical_drain, clears_full_release=False)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--physical-drain', action='store_true', help='require post-worker-join all-peer queue and 64-durable zero witness')
    parser.add_argument('--combined', action='store_true', help='require journal restart after each parent kill')
    parser.add_argument('--events', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    report = check([json.loads(line) for line in args.events.read_text().splitlines() if line.strip()], combined=args.combined, physical_drain=args.physical_drain)
    args.output.write_text(json.dumps(report, indent=2) + '\n')
