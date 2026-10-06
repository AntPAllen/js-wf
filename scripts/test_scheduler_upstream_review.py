#!/usr/bin/env python3
"""Exercise provenance rejection against actual completed cleanup evidence."""
import argparse
import copy
import importlib.util
import json
from pathlib import Path
from unittest.mock import patch


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--rc-root', type=Path, required=True)
    parser.add_argument('--stable-root', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    repo = Path(__file__).resolve().parents[1]
    spec = importlib.util.spec_from_file_location('reviewer', repo/'scripts/review-nats-scheduler-cleanup.py')
    reviewer = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(reviewer)
    rc, stable = args.rc_root.resolve(), args.stable_root.resolve()
    before = {str(root): reviewer.inventory(root) for root in (rc, stable)}
    positives = [reviewer.review(rc, repo, True, 'v2.15.1-RC.1'),
                 reviewer.review(stable, repo, True)]
    rejected = []

    def reject(label, change, expected='v2.15.1-RC.1'):
        source = copy.deepcopy(reviewer.read(rc/'source.json'))
        after = copy.deepcopy(reviewer.read(rc/'dependency-after.json'))
        change(source, after)
        original_read = reviewer.read

        def altered_read(path):
            if path == rc/'source.json': return source
            if path == rc/'dependency-after.json': return after
            return original_read(path)

        with patch.object(reviewer, 'read', altered_read):
            try:
                reviewer.review(rc, repo, True, expected)
            except (AssertionError, KeyError, ValueError):
                rejected.append(label)
            else:
                raise AssertionError('accepted altered provenance: '+label)

    reject('RC evidence rejected by default stable version', lambda s,a: None, 'v2.15.0')
    reject('empty dependency ledgers', lambda s,a: (s.update(dependency_before={}), a.clear()))
    reject('missing go.sum in both ledgers', lambda s,a: (s['dependency_before'].pop('go.sum'), a.pop('go.sum')))
    reject('extra dependency key in both ledgers', lambda s,a: (s['dependency_before'].update(other='0'*64), a.update(other='0'*64)))
    reject('dependency changed after execution', lambda s,a: a.update({'go.sum':'0'*64}))
    reject('equal ledgers differ from recorded Git source', lambda s,a: (s['dependency_before'].update({'go.mod':'0'*64}), a.update({'go.mod':'0'*64})))
    reject('production pin substituted', lambda s,a: s.update(production_version='v2.15.1-RC.1'))
    reject('download version substituted', lambda s,a: s['download'].update(Version='v2.15.0'))
    reject('download module substituted', lambda s,a: s['download'].update(Path='example.invalid/server'))
    reject('download error ignored', lambda s,a: s['download'].update(Error='download failed'))
    reject('checksum malformed', lambda s,a: s['download'].update(Sum='not-a-checksum'))
    reject('upstream origin substituted', lambda s,a: s['download']['Origin'].update(URL='https://example.invalid/server'))
    reject('upstream tag substituted', lambda s,a: s['download']['Origin'].update(Ref='refs/heads/main'))
    reject('upstream revision malformed', lambda s,a: s['download']['Origin'].update(Hash='main'))
    assert before == {str(root): reviewer.inventory(root) for root in (rc, stable)}
    result = dict(accepted=True, positive_reviews=positives, rejected_controls=rejected,
                  original_evidence_unchanged=True,
                  scope='Read-only actual stable/RC corpus checks and isolated metadata substitutions; no native rerun or broker store opened.')
    args.output.write_text(json.dumps(result, indent=2)+'\n')
    print(json.dumps(dict(accepted=True, positive_corpora=2, rejected_controls=len(rejected), original_evidence_unchanged=True)))


if __name__ == '__main__':
    main()
