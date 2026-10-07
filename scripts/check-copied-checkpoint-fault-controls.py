#!/usr/bin/env python3
"""Reject mutations of an actual retained copied-checkpoint fault proof."""
import argparse
import copy
import json
import tempfile
from pathlib import Path

import copied_checkpoint_fault_proof


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root', type=Path, required=True)
    parser.add_argument('--checkpoint', type=int, choices=(8520,4160), default=8520)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--state-watch-creation-stall', action='store_true')
    args = parser.parse_args()
    paths = list((args.root/'originals').rglob('copied-checkpoint-audit.json'))
    assert len(paths) == 1
    baseline = json.loads(paths[0].read_text())
    servers = json.loads((args.root/'actual-servers.json').read_text())
    identity = json.loads((args.root/'copied-store-admission.json').read_text())['identity']
    assert bool(baseline.get('state_creation_stall'))==args.state_watch_creation_stall
    fixture=paths[0].parent
    def validate(result,observed,wire_root=fixture):
        admitted=copied_checkpoint_fault_proof.validate(result,observed,identity,checkpoint=args.checkpoint)
        if args.state_watch_creation_stall:
            import copied_checkpoint_creation_proof
            admitted['creation']=copied_checkpoint_creation_proof.validate(wire_root,result,observed,identity)
        return admitted
    admitted = validate(baseline,servers)
    controls = []

    def reject(name, change):
        result, observed = copy.deepcopy(baseline), copy.deepcopy(servers)
        change(result, observed)
        try:
            validate(result, observed)
        except (AssertionError, KeyError, ValueError, TypeError):
            controls.append(name)
        else:
            raise AssertionError('invalid proof admitted: ' + name)

    if args.state_watch_creation_stall:
        for key,bad in [('creation_control_valid',False),('metadata_deadline','2030-01-01T00:00:00Z'),('attempts',1),('parent_budget_ns',30_000_000_000),('first_creation_elapsed_ns',1_000_000_000),
                        ('first_native_error','context deadline exceeded'),('server_id','wrong'),('server_name','wrong'),
                        ('upstream_url','nats://127.0.0.1:1'),('transport_joined','2030-01-01T00:00:00Z'),('frames',[])]:
            reject('creation-'+key,lambda r,s,k=key,v=bad:r['state_creation_stall'].__setitem__(k,v))
        reject('creation-missing',lambda r,s:r.pop('state_creation_stall'))
        for key,bad in [('forwarded_bytes',1),('subject','$JS.API.CONSUMER.CREATE.OTHER.x'),('disposition','released')]:
            reject('creation-pending-'+key,lambda r,s,k=key,v=bad:r['state_creation_stall']['pending_before_close'].__setitem__(k,v))
        reject('creation-proxy-unjoined',lambda r,s:r['state_creation_stall']['proxy_stats'].__setitem__('active_connections',1))
        reject('creation-trace-truncated',lambda r,s:r['state_creation_stall']['proxy_trace'].__setitem__('truncated',True))
        reject('creation-false-partial-barrier',lambda r,s:r['state_creation_stall']['frames'][2].__setitem__('initial_complete',True))
        reject('creation-partial-values',lambda r,s:r['state_creation_stall']['frames'][-1].__setitem__('included',1))
        reject('creation-parent-changed',lambda r,s:r['state_creation_stall']['frames'][0].__setitem__('deadline','2030-01-01T00:00:00Z'))
        reject('creation-residual-state-consumer',lambda r,s:r['readiness_after']['KV_WF_STATE']['state'].__setitem__('consumer_count',1))
        reject('creation-metadata-omitted',lambda r,s:r['state_creation_stall'].pop('metadata_lookup'))
        reject('creation-metadata-failed',lambda r,s:r['state_creation_stall']['metadata_lookup'].__setitem__('error','timeout'))
        reject('creation-metadata-outside-audit',lambda r,s:r['state_creation_stall']['metadata_lookup'].__setitem__('started','2000-01-01T00:00:00Z'))
        reject('creation-metadata-after-deadline',lambda r,s:r['state_creation_stall']['metadata_lookup'].__setitem__('returned','2030-01-01T00:00:00Z'))
        wire=(fixture/'creation-wire.jsonl').read_bytes()
        for name,changed in [('wire-truncated',wire[:-8]),('wire-missing',b''),('wire-invalid-extra-record',wire+b'{}\n')]:
            with tempfile.TemporaryDirectory(prefix='copied-creation-proof-control-') as temp:
                scratch=Path(temp);(scratch/'creation-wire.jsonl').write_bytes(changed)
                try:validate(copy.deepcopy(baseline),copy.deepcopy(servers),scratch)
                except (AssertionError,KeyError,ValueError,TypeError):controls.append('creation-'+name)
                else:raise AssertionError('invalid wire accepted: '+name)
        assert (fixture/'creation-wire.jsonl').read_bytes()==wire
    if args.checkpoint==4160:
        reject('missing-inherited-cursors',lambda r,s:r.__setitem__('inherited_cursors',[]))
        reject('unknown-inherited-consumer',lambda r,s:r['inherited_cursors'][0].__setitem__('name','application'))
        reject('inherited-non-memory',lambda r,s:r['inherited_cursors'][0]['config'].__setitem__('mem_storage',False))
        reject('inherited-new-consumer',lambda r,s:r['inherited_cursors'][0].__setitem__('created','2030-01-01T00:00:00Z'))
        reject('inherited-delete-failed',lambda r,s:r['inherited_deletions'][0].__setitem__('error','failed'))
        reject('inherited-data-changed',lambda r,s:r['readiness_before_cleanup']['WF_INV']['state'].__setitem__('messages',1))
        reject('inherited-cleanup-after-trial',lambda r,s:r.__setitem__('inherited_cleanup_finished','2030-01-01T00:00:00Z'))
    for field, value in (
        ('cursor_owner_restart', False), ('parallel_decode', False), ('qualifies_24h', True),
        ('cutoff', baseline['cutoff']-1), ('error', 'context deadline exceeded'), ('audit_ns', 0),
        ('elapsed_ns', 20_000_000_000), ('journal_visits', baseline['journal_visits']-1),
        ('cleanup', {'WF_INV': 0, 'WF_JRN': 1}), ('state_watches', []),
        ('readiness_after', {}), ('restart_completed', '2000-01-01T00:00:00Z'),
    ):
        reject(field, lambda r, s, key=field, bad=value: r.__setitem__(key, bad))
    for field in ('Invocations', 'Journals', 'Entries', 'Terminal'):
        reject('incomplete-' + field, lambda r, s, key=field: r['report'].__setitem__(key, 0 if key=='Entries' and args.checkpoint==4160 else r['report'][key]-1))
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
                         ('attempts', 1), ('buffered_before_exposure', baseline['cutoff']),
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
    if (baseline.get('state_connection_loss') or {}).get('peer_outage'):
        for key, bad in (('peer_outage', False), ('exposed_status', 'CONNECTED'),
                         ('disconnected', '2000-01-01T00:00:00Z'), ('disconnect_error', ''),
                         ('offline_hold_ns', 2_000_000_000), ('restart_started', '2000-01-01T00:00:00Z')):
            reject('peer-outage-' + key, lambda r, s, k=key, value=bad: r['state_connection_loss'].__setitem__(k, value))
        reject('closed-watch-substituted-for-idle', lambda r, s: attempt_frames(r)[0].__setitem__('error', 'closed before initial completion'))
        reject('missing-timeout-decision', lambda r, s: r['state_connection_loss'].__setitem__('frames', [f for f in r['state_connection_loss']['frames'] if f['event'] != 'watch_idle_timeout']))
        reject('timeout-decision-after-heal', lambda r, s: next(f for f in r['state_connection_loss']['frames'] if f['event']=='watch_idle_timeout').__setitem__('time', '2100-01-01T00:00:00Z'))
        reject('premature-idle-timeout', lambda r, s: attempt_frames(r)[0].__setitem__('elapsed_ns', 1_000_000_000))
    report = dict(admitted=admitted, rejected_controls=controls, rejected_count=len(controls),
                  scope='Mutations of actual retained fault result and server observations; no broker or original store opened.')
    args.output.write_text(json.dumps(report, indent=2)+'\n')
    print(json.dumps(dict(rejected_count=len(controls), source='actual retained fault proof')))


if __name__ == '__main__':
    main()
