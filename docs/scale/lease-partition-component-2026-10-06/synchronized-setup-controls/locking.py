#!/usr/bin/env python3
"""Correct only observed unlocked addPeer setup in two pinned upstream tests."""
import argparse
import hashlib
import json
from pathlib import Path
import re


def transform(original):
    updated = original
    changes = []
    expected = {'TestNRGTruncateWALRevertsUncommittedRemovePeer': (1, 2), 'TestNRGEvictPeers': (7, 15)}
    pattern = r'(?m)^((\t+)n\.addPeer\([^\n]+\)\n(?:\2n\.addPeer\([^\n]+\)\n)*)'
    reverse = r'(?m)^(\t+)n\.Lock\(\)\n((?:\1n\.addPeer\([^\n]+\)\n)+)\1n\.Unlock\(\)\n'
    for name, counts in expected.items():
        start = updated.index('func '+name+'(')
        end = updated.find('\nfunc ', start+1)
        if end < 0:
            raise ValueError('missing pinned function boundary')
        body = updated[start:end]
        matches = list(re.finditer(pattern, body))
        if (len(matches), body.count('n.addPeer(')) != counts:
            raise ValueError('unexpected peer setup shape: '+name)
        modified = re.sub(pattern, lambda m: m[2]+'n.Lock()\n'+m[1]+m[2]+'n.Unlock()\n', body)
        if re.sub(reverse, lambda m: m[2], modified) != body:
            raise ValueError('change exceeds lock/unlock of peer additions')
        changes.append({'test': name, 'groups': counts[0], 'calls': counts[1]})
        updated = updated[:start]+modified+updated[end:]
    return updated, changes


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--original', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    if args.output.exists() or args.output.resolve() == args.original.resolve():
        parser.error('require a fresh separate output')
    original = args.original.read_text()
    updated, changes = transform(original)
    args.output.write_text(updated)
    print(json.dumps({'original_sha256': hashlib.sha256(original.encode()).hexdigest(),
                      'modified_sha256': hashlib.sha256(updated.encode()).hexdigest(), 'changes': changes,
                      'scope': 'Eight lock/unlock pairs around 17 peer additions in two upstream tests. Original assertions, cases, sleeps, deadlines and production sources unchanged.'}))


if __name__ == '__main__':
    main()
