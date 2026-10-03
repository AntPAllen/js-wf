#!/usr/bin/env python3
"""Verify every archived byte and characterize failed gaps without qualifying a row."""
import argparse, hashlib, io, json, tarfile
from pathlib import Path

def review(root):
    terminal = json.loads((root/'terminal.json').read_text())
    assert terminal['conclusion'] == 'failure'
    assert terminal['headSha'] == '9ac3ad44267b68d1a1ec63ce47e1fb11d9c00204'
    parts = sorted(root.glob('rolling-originals.tar.gz.part-*'))
    archive = b''.join(p.read_bytes() for p in parts)
    expected = json.loads((root/'archive-parts.json').read_text())
    assert {p.name:hashlib.sha256(p.read_bytes()).hexdigest() for p in parts} == expected['parts']
    assert hashlib.sha256(archive).hexdigest() == expected['archive_sha256']
    manifest = json.loads((root/'rolling-manifest.json').read_text())['files']
    with tarfile.open(fileobj=io.BytesIO(archive)) as tar:
        members = tar.getmembers()
        assert len(members) == len(manifest) and all(m.isfile() for m in members)
        hashes = {m.name:hashlib.sha256(tar.extractfile(m).read()).hexdigest() for m in members}
        assert hashes == manifest
        def load(name):return json.load(tar.extractfile(name))
        repairs = load('repairs.json') or []
        gaps = []
        for n in range(1,4):
            base = f'fault-{n}-start-gap/'
            gap = load(base+'upgrade-gap.json')
            proof = gap['proof']
            assert proof['signal'] == 'SIGKILL' and proof['run_messages'] == 0 and proof['journal_absent']
            assert proof['retained'] == gap['after_upgrade'] == proof['receipt']['invocation']
            events = [r for r in repairs if r['kind'] == 'start' and r['type'] == gap['type'] and r['id'] == gap['id']]
            completed = base+'completion.json' in manifest
            if n == 3:
                assert proof['retained']['Sequence'] == 1270 and not completed and not events
            else:
                completion = load(base+'completion.json')
                assert completion['kill_to_terminal_ns'] < 30000000000 and any(r['outcome']=='acknowledged' for r in events)
            gaps.append(dict(invocation_sequence=proof['retained']['Sequence'],killed=proof['killed'],repair_released=gap['repair_released'],repair_events=len(events),completion=load(base+'completion.json') if completed else None))
        assert 'start-scans.json' not in manifest
    return dict(accepted_release_row=False,source_commit=terminal['headSha'],verified_members=len(members),gaps=gaps,
        conclusion='Both initial gaps complete; third has no recorded repair attempt before failure. No scan progress was retained, so exact cursor/server cause remains unconfirmed.')

if __name__=='__main__':
    parser=argparse.ArgumentParser();parser.add_argument('root',type=Path);parser.add_argument('--output',type=Path);args=parser.parse_args()
    output=json.dumps(review(args.root),indent=2)+'\n'
    if args.output:args.output.write_text(output)
    else:print(output,end='')
