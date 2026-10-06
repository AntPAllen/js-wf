#!/usr/bin/env python3
"""Run an R5 row beyond hosted limits from an isolated checkout and retained binary.

A single row never qualifies the full matrix or full Tier3 release.
"""
import argparse
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import subprocess
import tarfile
import time

REPO = Path(__file__).resolve().parents[1]
MEMORY_LIMITS = ('512MiB', '1GiB', '2GiB', '4GiB')


def load_rows():
    spec = importlib.util.spec_from_file_location('soak_rows', REPO/'scripts/check-tier3-journal-row.py')
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module.TESTS


def execution(row, duration, seed, fixture, shutdown, gap, retained_audit_trace=False, batched_retained_audit=False, memory_limit='512MiB', streaming_state_retained_audit=False, explicit_route_seeds=False, journal_rollout='none', audit_wait_stack=False, concurrent_state_retained_audit=False, chunked_state_retained_audit=False, cached_latency_metadata=False):
    if row not in load_rows() or duration not in ('35s', '10m', '24h'):
        raise ValueError('unsupported row or duration')
    if type(seed) is not int or not 1 <= seed <= 2**63-1:
        raise ValueError('seed must be positive int64')
    if shutdown not in ('sigkill', 'ldm') or (row != 'rolling_upgrade' and (gap or shutdown != 'sigkill')):
        raise ValueError('invalid shutdown or gap profile')
    if sum((batched_retained_audit, streaming_state_retained_audit, concurrent_state_retained_audit, chunked_state_retained_audit)) > 1:
        raise ValueError('conflicting retained audit modes')
    if memory_limit not in MEMORY_LIMITS:
        raise ValueError('unsupported explicit memory budget')
    if chunked_state_retained_audit and memory_limit != '4GiB':
        raise ValueError('chunked audit profile requires explicit 4GiB memory budget')
    if audit_wait_stack and not retained_audit_trace:
        raise ValueError('audit wait stack requires retained audit trace')
    if cached_latency_metadata and row.startswith('server_clock_'):
        raise ValueError('cached latency metadata applies to point audits, not clock controller rows')
    # Do not inherit fixture/mutation/child-process activation from another run.
    env = {k:v for k,v in os.environ.items() if not k.startswith('WF_')}
    env.update(GOMEMLIMIT=memory_limit, GOMAXPROCS='2', WF_TIER3_MATRIX='1',
               WF_TIER3_MATRIX_DURATION=duration, WF_TIER3_SYNC_INTERVAL='2m',
               TIER3_MATRIX_ARTIFACT_ROOT=str(fixture), FAULT_SEED=str(seed))
    if retained_audit_trace:
        env['WF_TIER3_RETAINED_AUDIT_TRACE'] = '1'
    if audit_wait_stack:
        env['WF_TIER3_AUDIT_WAIT_STACK'] = '1'
    if batched_retained_audit:
        env['WF_TIER3_BATCHED_RETAINED_AUDIT'] = '1'
    if streaming_state_retained_audit:
        env['WF_TIER3_STREAMING_STATE_RETAINED_AUDIT'] = '1'
    if concurrent_state_retained_audit:
        env['WF_TIER3_CONCURRENT_STATE_RETAINED_AUDIT'] = '1'
    if chunked_state_retained_audit:
        env.update(WF_TIER3_CHUNKED_STATE_RETAINED_AUDIT='1', GOMAXPROCS='4', GOGC='500')
    if cached_latency_metadata:
        env['WF_MATRIX_CACHED_LATENCY_METADATA']='1'
    if explicit_route_seeds:
        env['WF_TIER3_EXPLICIT_ROUTE_SEEDS']='1'
    flags = ['--require-checkpoint-audits']
    if journal_rollout != 'none':
        if journal_rollout != 'protobuf-to-json' or row != 'worker_kill':
            raise ValueError('journal rollout requires worker_kill')
        env['WF_MATRIX_JOURNAL_ROLLOUT']=journal_rollout
        flags += ['--require-journal-rollout']
    if row == 'rolling_upgrade':
        env['WF_TIER3_UPGRADE_SHUTDOWN'] = shutdown
        flags += ['--expected-upgrade-shutdown', shutdown]
        if gap:
            env['WF_TIER3_UPGRADE_START_GAP'] = '1'
            flags += ['--require-upgrade-start-gap', '--require-start-scan-progress']
    if row.startswith('server_clock_'):
        env.update(WF_TIER3_CLOCK_TIMER_CUT='1', WF_TIER3_COMMON_CLOCK='1')
        flags += ['--require-clock-timer-cut', '--require-common-timer-clock']
    test = load_rows()[row]
    if row == 'worker_clock':
        test = '('+test+'|TestTier3WorkerClockNormalizationPreservesRawEvidence)'
    # Reserve ample cleanup time past the harness's duration+6m context.
    timeout = '24h20m' if duration == '24h' else '20m'
    return env, ['-test.run=^'+test+'$', '-test.v=test2json', '-test.count=1',
                 '-test.timeout='+timeout], flags


def sha(path):
    digest = hashlib.sha256()
    with path.open('rb') as stream:
        for block in iter(lambda: stream.read(1024*1024), b''):
            digest.update(block)
    return digest.hexdigest()


def archive_originals(root):
    # Include the actual compiled checkout, binary, broker stores and raw logs.
    # .git is only a pointer to the parent repository, not execution evidence.
    files = sorted(p for p in root.rglob('*') if p.is_file() and p.name != '.git'
                   and '__pycache__' not in p.parts and p.name not in
                   ('originals.tmp.tar.gz', 'originals.tar.gz', 'archive-manifest.json'))
    hashes = {str(p.relative_to(root)):sha(p) for p in files}
    temporary = root/'originals.tmp.tar.gz'
    with tarfile.open(temporary, 'w:gz') as tar:
        for path in files:
            tar.add(path, arcname=str(path.relative_to(root)))
    with tarfile.open(temporary) as tar:
        observed = {}
        for member in tar.getmembers():
            if not member.isfile():
                raise ValueError('unexpected non-file archive member')
            digest = hashlib.sha256()
            with tar.extractfile(member) as stream:
                for block in iter(lambda:stream.read(1024*1024), b''):
                    digest.update(block)
            observed[member.name] = digest.hexdigest()
        if observed != hashes or len(observed) != len(files):
            raise ValueError('archive member readback mismatch')
    temporary.rename(root/'originals.tar.gz')
    (root/'archive-manifest.json').write_text(json.dumps(dict(files=hashes,
        archive_sha256=sha(root/'originals.tar.gz')),indent=2)+'\n')


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--root', type=Path, required=True)
    p.add_argument('--row', choices=tuple(load_rows()), required=True)
    p.add_argument('--duration', choices=('35s','10m','24h'), default='24h')
    p.add_argument('--seed', type=int, default=1)
    p.add_argument('--upgrade-shutdown', choices=('sigkill','ldm'), default='sigkill')
    p.add_argument('--upgrade-start-gap', action='store_true')
    p.add_argument('--retained-audit-trace', action='store_true')
    p.add_argument('--audit-wait-stack', action='store_true', help='Diagnostic: capture parent stack and trace one second before a pending audit deadline')
    readers = p.add_mutually_exclusive_group()
    readers.add_argument('--chunked-state-retained-audit', action='store_true', help='Experimental: bounded chunk/R1 audit with concurrent state; requires 4GiB and selects 4 CPUs/GOGC500')
    readers.add_argument('--concurrent-state-retained-audit', action='store_true', help='Experimental: overlap complete state snapshot and journal reads within original audit limits')
    readers.add_argument('--batched-retained-audit', action='store_true')
    readers.add_argument('--streaming-state-retained-audit', action='store_true')
    p.add_argument('--cached-latency-metadata', action='store_true', help='Experimental: reuse successful final point audit metadata handles; all record reads remain fresh')
    p.add_argument('--explicit-route-seeds', action='store_true', help='Diagnostic: seed every other route-only peer on each restart')
    p.add_argument('--journal-rollout', choices=('none','protobuf-to-json'), default='none')
    p.add_argument('--no-race', action='store_true')
    p.add_argument('--memory-limit', choices=MEMORY_LIMITS, default='512MiB',
                   help='Explicit Go memory budget, captured with execution evidence')
    a = p.parse_args()
    root = a.root.resolve()
    if root.exists() or root.is_relative_to(REPO):
        p.error('root must be fresh and outside the repository')
    env, testargs, flags = execution(a.row,a.duration,a.seed,root/'fixture',
                                     a.upgrade_shutdown,a.upgrade_start_gap,
                                     a.retained_audit_trace,a.batched_retained_audit,a.memory_limit,a.streaming_state_retained_audit,a.explicit_route_seeds,a.journal_rollout,a.audit_wait_stack,a.concurrent_state_retained_audit,a.chunked_state_retained_audit,a.cached_latency_metadata)
    if subprocess.check_output(['git','status','--porcelain'],cwd=REPO):
        p.error('execution requires a clean committed checkout')
    revision = subprocess.check_output(['git','rev-parse','HEAD'],cwd=REPO,text=True).strip()
    root.mkdir(parents=True)
    commands = []
    source = root/'source'
    state = dict(source=revision,row=a.row,seed=a.seed,duration=a.duration,
                 status='preparing',supervisor_pid=os.getpid(),started=time.time(),
                 upgrade_shutdown=a.upgrade_shutdown if a.row=='rolling_upgrade' else None,
                 upgrade_start_gap=a.upgrade_start_gap,race=not a.no_race,
                 memory_limit=a.memory_limit, gomaxprocs=env['GOMAXPROCS'], gc_percent=env.get('GOGC'),
                 retained_audit_trace=a.retained_audit_trace,
                 audit_wait_stack=a.audit_wait_stack,
                 batched_retained_audit=a.batched_retained_audit,
                 streaming_state_retained_audit=a.streaming_state_retained_audit,
                 concurrent_state_retained_audit=a.concurrent_state_retained_audit,
                 chunked_state_retained_audit=a.chunked_state_retained_audit,
                 cached_latency_metadata=a.cached_latency_metadata,
                 explicit_route_seeds=a.explicit_route_seeds,journal_rollout=a.journal_rollout,
                 clears_full_tier3_release=False)
    def save():
        temporary=root/'execution.tmp.json'
        temporary.write_text(json.dumps(state,indent=2)+'\n')
        temporary.replace(root/'execution.json')
    def call(command, cwd=source, **kwargs):
        commands.append(dict(command=command,working_directory=str(cwd)))
        (root/'commands.json').write_text(json.dumps(commands,indent=2)+'\n')
        return subprocess.run(command,cwd=cwd,env=kwargs.pop('env',env),check=True,**kwargs)
    save()
    child = None
    try:
        # Keep source stable while the main checkout continues progressing.
        call(['git','worktree','add','--detach','--no-checkout',str(source),revision],cwd=REPO)
        names=subprocess.check_output(['git','ls-tree','-r','--name-only',revision],cwd=REPO,text=True).splitlines()
        included=[n for n in names if not n.startswith('docs/') or n.endswith(('.go','.py','.yml'))]
        (root/'checkout-files.json').write_text(json.dumps(included,indent=2)+'\n')
        call(['git','sparse-checkout','set','--no-cone','--stdin'],input=''.join('/'+n+'\n' for n in included),text=True)
        call(['git','read-tree','-mu','HEAD'])
        selected=[n for n in names if n.endswith(('.go','.py','.yml')) or n in ('go.mod','go.sum') or n.startswith('sim/testdata/')]
        def inventory():
            assert not subprocess.check_output(['git','status','--porcelain'],cwd=source)
            assert subprocess.check_output(['git','rev-parse','HEAD'],cwd=source,text=True).strip()==revision
            return dict(revision=revision,files={n:sha(source/n) for n in selected})
        before=inventory()
        (root/'source-before.json').write_text(json.dumps(before,indent=2)+'\n')
        call(['docker','info'],stdout=(root/'docker-info.txt').open('w'))
        binary=root/'integration.test'
        call(['go','test','-p=1',* ([] if a.no_race else ['-race']),'-buildvcs=true','-c','-o',str(binary),'./integration'])
        info=subprocess.check_output(['go','version','-m',str(binary)],text=True)
        assert ('-race=true' in info)==(not a.no_race)
        assert f'vcs.revision={revision}' in info and 'vcs.modified=false' in info
        (root/'binary.json').write_text(json.dumps(dict(sha256=sha(binary),race=not a.no_race,build_info=info),indent=2)+'\n')
        if a.row=='rolling_upgrade':
            old=root/'old-server';old.mkdir()
            call(['go','install','github.com/nats-io/nats-server/v2@v2.11.17'],env=dict(env,CGO_ENABLED='0',GOBIN=str(old)))
            env['WF_NATS_SERVER_BIN']=str(old/'nats-server')
        observed=a.row in ('worker_clock','rolling_upgrade','server_clock_ahead','server_clock_behind')
        def capture(stage):
            call(['python3','scripts/capture-tier3-clock-source.py','--row',a.row,'--root',str(root/'fixture'),'--stage',stage])
        if observed:capture('before')
        (root/'test-environment.json').write_text(json.dumps({k:v for k,v in env.items() if k.startswith('WF_') or k in ('GOMEMLIMIT','GOMAXPROCS','GOGC','FAULT_SEED','TIER3_MATRIX_ARTIFACT_ROOT')},indent=2)+'\n')
        command=['go','tool','test2json','-t','-p','js-wf/integration',str(binary),*testargs]
        commands.append(dict(command=command,working_directory=str(source/'integration')))
        (root/'commands.json').write_text(json.dumps(commands,indent=2)+'\n')
        with (root/'events.jsonl').open('w') as out,(root/'stderr.log').open('w') as err:
            child=subprocess.Popen(command,cwd=source/'integration',env=env,stdout=out,stderr=err)
            state.update(status='running',test_pid=child.pid);save()
            status=child.wait()
        state['test_exit_code']=status
        if observed:capture('after')
        after=inventory();(root/'source-after.json').write_text(json.dumps(after,indent=2)+'\n');assert after==before
        assert sha(binary)==json.loads((root/'binary.json').read_text())['sha256']
        if status:raise RuntimeError(f'named test failed with exit{status}')
        call(['python3','scripts/check-tier3-journal-row.py','--root',str(root/'fixture'),
              '--events',str(root/'events.jsonl'),'--row',a.row,'--duration',a.duration,
              '--expected-seed',str(a.seed),'--output',str(root/'result.json'),*flags])
        for script,name in [('explain-tier3-events.py','event-explanations.json'),('review-tier3-fencing.py','fencing-timeline-review.json')]:
            call(['python3','scripts/'+script,'--root',str(root/'fixture'),'--output',str(root/'fixture'/name)])
        state['status']='row_verified'
    except BaseException as error:
        live = child is not None and child.poll() is None
        state.update(status='supervisor_interrupted' if live else 'failed',error=str(error))
        raise
    finally:
        state['finished']=time.time();save()
        # KeyboardInterrupt while a child is live must leave its original root
        # intact for observation, never archive changing stores as final proof.
        if child is None or child.poll() is not None:
            archive_originals(root)
    print(json.dumps(state,indent=2))


if __name__=='__main__':main()
