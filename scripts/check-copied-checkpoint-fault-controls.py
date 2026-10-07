#!/usr/bin/env python3
"""Reject mutations of an actual retained copied-checkpoint fault proof."""
import argparse
import copy
import json
from pathlib import Path

import copied_checkpoint_fault_proof


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    paths = list((args.root/'originals').rglob('copied-checkpoint-audit.json'))
    assert len(paths) == 1
    baseline = json.loads(paths[0].read_text())
    servers = json.loads((args.root/'actual-servers.json').read_text())
    identity = json.loads((args.root/'copied-store-admission.json').read_text())['identity']
    admitted = copied_checkpoint_fault_proof.validate(baseline, servers, identity)
    controls = []

    def reject(name, change):
        result, observed = copy.deepcopy(baseline), copy.deepcopy(servers)
        change(result, observed)
        try:
            copied_checkpoint_fault_proof.validate(result, observed, identity)
        except (AssertionError, KeyError, ValueError, TypeError):
            controls.append(name)
        else:
            raise AssertionError('invalid proof admitted: ' + name)

    for field, value in (
        ('cursor_owner_restart', False), ('parallel_decode', False), ('qualifies_24h', True),
        ('cutoff', 238559), ('error', 'context deadline exceeded'), ('audit_ns', 0),
        ('elapsed_ns', 20_000_000_000), ('journal_visits', 2632931),
        ('cleanup', {'WF_INV': 0, 'WF_JRN': 1}), ('state_watches', []),
        ('readiness_after', {}), ('restart_completed', '2000-01-01T00:00:00Z'),
    ):
        reject(field, lambda r, s, key=field, bad=value: r.__setitem__(key, bad))
    for field in ('Invocations', 'Journals', 'Entries', 'Terminal'):
        reject('incomplete-' + field, lambda r, s, key=field: r['report'].__setitem__(key, r['report'][key]-1))
    reject('source-hole', lambda r, s: r['readiness']['WF_JRN']['state'].__setitem__('num_deleted', 1))
    reject('source-bound', lambda r, s: r['readiness']['WF_JRN']['state'].__setitem__('last_seq', 1))
    reject('watch-error', lambda r, s: r['state_watches'][-1].__setitem__('StopError', 'failed'))
    reject('watch-unjoined', lambda r, s: r['state_watches'][0].__setitem__('StopFinishedNS', r['elapsed_ns']+1))
    reject('source-not-current', lambda r, s: next(iter(r['readiness_after'].values()))['cluster']['replicas'][0].__setitem__('current', False))
    reject('non-R5-source', lambda r, s: next(iter(r['readiness_after'].values()))['config'].__setitem__('num_replicas', 1))
    for field, value in (('num_replicas', 3), ('mem_storage', False), ('ack_policy', 'explicit')):
        reject('target-' + field, lambda r, s, key=field, bad=value: r['target']['config'].__setitem__(key, bad))
    reject('no-pending-at-cut', lambda r, s: r['target'].__setitem__('num_pending', 0))
    reject('wrong-owner', lambda r, s: r['target']['cluster'].__setitem__('leader', 'unknown'))
    reject('no-source-exit', lambda r, s: r['kill'].__setitem__('source_stopped', '0001-01-01T00:00:00Z'))
    reject('source-running', lambda r, s: r['kill'].__setitem__('state', 'running'))
    reject('kill-reply-before-start', lambda r, s: r['kill'].__setitem__('kill_returned', '2000-01-01T00:00:00Z'))
    reject('no-concurrent-exit-observation', lambda r, s: r['kill'].__setitem__('concurrent_observation', False))
    reject('no-replacement-process', lambda r, s: s.pop())
    def replacement_delete(result):
        replacement = next(c['info']['name'] for c in result['cursors']
                           if c['created'] and c['info']['stream_name'] == 'WF_JRN'
                           and c['info']['name'] != result['target']['name'])
        return next(d for d in result['deletions'] if d['name'] == replacement)

    reject('residual-new-cursor', lambda r, s: replacement_delete(r).__setitem__('error', 'failed'))
    reject('unknown-old-delete-error', lambda r, s: next(d for d in r['deletions'] if d['name'] == r['target']['name']).__setitem__('error', 'failed'))
    reject('missing-delete', lambda r, s: r['deletions'].pop())
    reject('reused-cursor', lambda r, s: [c for c in r['cursors'] if c['created'] and c['info']['stream_name']=='WF_JRN'][-1]['info'].__setitem__('name', r['target']['name']))
    reject('wrong-resume-sequence', lambda r, s: [c for c in r['cursors'] if c['created'] and c['info']['stream_name']=='WF_JRN'][-1]['info']['config'].__setitem__('opt_start_seq', 1))
    owner_name = identity + '-n' + str(baseline['kill']['node'])
    owner_indices = [i for i, s in enumerate(servers) if s['args'][s['args'].index('-n')+1]==owner_name]
    before, after = sorted(owner_indices, key=lambda i: servers[i]['observed_utc'])
    reject('same-container', lambda r, s: s[after]['container'].__setitem__('Id', s[before]['container']['Id']))
    reject('same-process', lambda r, s: s[after].__setitem__('pid', s[before]['pid']))
    reject('changed-store', lambda r, s: [m for m in s[after]['container']['Mounts'] if m['Destination']=='/data'][0].__setitem__('Source', '/tmp/unverified'))
    if baseline.get('state_connection_loss'):
        for key, bad in (('owner', -1), ('server_id', ''), ('server_name', 'unknown'),
                         ('url', 'nats://127.0.0.1:1'), ('status_after_close', 'CONNECTED'),
                         ('attempts', 1), ('buffered_before_exposure', 238560),
                         ('close_finished', '2000-01-01T00:00:00Z'), ('frames', [])):
            reject('watch-connection-' + key, lambda r, s, k=key, value=bad: r['state_connection_loss'].__setitem__(k, value))
        def attempt_frames(r):
            return [f for f in r['state_connection_loss']['frames'] if f['event'] == 'attempt_return']
        reject('partial-watch-certified', lambda r, s: attempt_frames(r)[0].__setitem__('initial_complete', True))
        reject('partial-watch-error-hidden', lambda r, s: attempt_frames(r)[0].__setitem__('error', ''))
        reject('fresh-watch-incomplete', lambda r, s: attempt_frames(r)[-1].__setitem__('initial_complete', False))
        reject('fresh-watch-too-small', lambda r, s: attempt_frames(r)[-1].__setitem__('received', 238559))
        reject('watch-deadline-reset', lambda r, s: attempt_frames(r)[-1].__setitem__('deadline', '2100-01-01T00:00:00Z'))
        reject('unknown-closed-watch-stop', lambda r, s: r['state_watches'][0].__setitem__('StopError', 'failed'))
        def change_complete_frames(r, field, value):
            for f in r['state_connection_loss']['frames']:
                if f['initial_complete']:
                    f[field] = value
        reject('missing-excluded-physical-state', lambda r, s: change_complete_frames(r, 'received', 238560))
        reject('incomplete-included-state', lambda r, s: change_complete_frames(r, 'included', 238559))
        reject('missing-native-barrier', lambda r, s: r['state_connection_loss'].__setitem__('frames', [f for f in r['state_connection_loss']['frames'] if f['event'] != 'initial_complete']))
    report = dict(admitted=admitted, rejected_controls=controls, rejected_count=len(controls),
                  scope='Mutations of actual retained fault result and server observations; no broker or original store opened.')
    args.output.write_text(json.dumps(report, indent=2)+'\n')
    print(json.dumps(dict(rejected_count=len(controls), source='actual retained fault proof')))


if __name__ == '__main__':
    main()
