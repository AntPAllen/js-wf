#!/usr/bin/env python3
"""Reproduce missing-source scheduler cleanup with fresh two-message stores."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess

REPO = Path(__file__).resolve().parents[1]
TEST = 'TestWorkflowMissingSourceScheduleCleanupPersists'

def hashes(root):
    return {str(p.relative_to(root)): hashlib.sha256(p.read_bytes()).hexdigest()
            for p in sorted(root.rglob('*')) if p.is_file()}

def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--root', type=Path, required=True)
    a = p.parse_args()
    root = a.root.resolve()
    assert not root.exists() and not root.is_relative_to(REPO)
    version = subprocess.check_output(['go','list','-m','-f','{{.Version}}','github.com/nats-io/nats-server/v2'],cwd=REPO,text=True).strip()
    assert version == 'v2.15.0'
    module = Path(subprocess.check_output(['go','list','-m','-f','{{.Dir}}','github.com/nats-io/nats-server/v2'],cwd=REPO,text=True).strip())
    upstream = hashes(module)
    fixture = REPO/'scripts/fixtures/nats-scheduler-cleanup_minimal_test.go.txt'
    fixture_raw = fixture.read_bytes()
    fixture_hash = hashlib.sha256(fixture_raw).hexdigest()
    root.mkdir(parents=True)
    (root/'runner.py').write_bytes(Path(__file__).read_bytes())
    (root/'fixture.go.txt').write_bytes(fixture_raw)
    (root/'module-before.json').write_text(json.dumps(upstream,indent=2)+'\n')
    (root/'source.json').write_text(json.dumps(dict(version=version,module=str(module),revision=subprocess.check_output(['git','rev-parse','HEAD'],cwd=REPO,text=True).strip(),clean=not bool(subprocess.check_output(['git','status','--porcelain'],cwd=REPO)),fixture_sha256=fixture_hash,runner_sha256=hashlib.sha256(Path(__file__).read_bytes()).hexdigest()),indent=2)+'\n')
    results = []
    try:
        for mode in ('baseline','dirty-control'):
            out = root/mode
            out.mkdir()
            build = out/'nats-source'
            shutil.copytree(module,build)
            assert hashes(build) == upstream
            (build/'server').chmod(0o755)
            (build/'server/workflow_missing_source_cleanup_test.go').write_bytes(fixture_raw)
            if mode == 'dirty-control':
                target = build/'server/filestore.go'
                baseline = target.read_text()
                anchor = '\tfs.scheduling.running = true\n\n\tscheduledMsgs := fs.scheduling.getScheduledMessages('
                assert baseline.count(anchor) == 1
                modified = baseline.replace(anchor,'\tfs.scheduling.running = true\n\tpriorScheduleCount := len(fs.scheduling.schedules)\n\n\tscheduledMsgs := fs.scheduling.getScheduledMessages(')
                point = modified.index('\n\tif len(scheduledMsgs) > 0 {',modified.index('func (fs *fileStore) runMsgScheduling()'))
                modified = modified[:point]+'\n\tif len(fs.scheduling.schedules) != priorScheduleCount {\n\t\tfs.dirty++\n\t}\n'+modified[point:]
                assert not target.is_symlink() and target.resolve().is_relative_to(build.resolve())
                target.chmod(target.stat().st_mode|0o200)
                target.write_text(modified)
                (out/'baseline-filestore.go.txt').write_text(baseline)
                (out/'dirty-count-control.go.txt').write_text(modified)
            compiled = hashes(build)
            assert compiled.pop('server/workflow_missing_source_cleanup_test.go') == fixture_hash
            changed = {n for n,h in compiled.items() if upstream.get(n) != h}
            assert set(compiled) == set(upstream)
            assert changed == ({'server/filestore.go'} if mode == 'dirty-control' else set())
            (out/'compiled-inventory.json').write_text(json.dumps(hashes(build),indent=2)+'\n')
            cmd = ['go','test','-p=1','-json','./server','-run','^'+TEST+'$','-count=1','-timeout=3m']
            env = dict(os.environ,GOWORK='off',GOMEMLIMIT='512MiB',GOMAXPROCS='2',WF_SCHEDULER_CLEANUP_STORE=str(out/'store'))
            (out/'command.json').write_text(json.dumps(dict(command=cmd,working_directory=str(build),environment={k:env[k] for k in ('GOWORK','GOMEMLIMIT','GOMAXPROCS','WF_SCHEDULER_CLEANUP_STORE')}),indent=2)+'\n')
            with (out/'events.jsonl').open('w') as stdout, (out/'stderr.log').open('w') as stderr:
                run = subprocess.run(cmd,cwd=build,env=env,stdout=stdout,stderr=stderr,timeout=600)
            events = [json.loads(l) for l in (out/'events.jsonl').read_text().splitlines()]
            assert not any(e['Action'] in ('skip','build-fail') for e in events)
            assert sum(e['Action']=='run' and e.get('Test')==TEST for e in events) == 1
            verdict = 'fail' if mode == 'baseline' else 'pass'
            assert [e['Action'] for e in events if e.get('Test')==TEST and e['Action'] in ('pass','fail','skip')] == [verdict]
            assert [e['Action'] for e in events if not e.get('Test') and e['Action'] in ('pass','fail','skip')] == [verdict]
            assert run.returncode == (1 if mode == 'baseline' else 0)
            outputs = ''.join(e.get('Output','') for e in events)
            measured = 'before=1 after=0 reopened='+('1' if mode=='baseline' else '0')+' callbacks=0 physical=1 last=2'
            assert 'CLEANUP_RESULT fresh_source_count=1 anchor_count=1 '+measured in outputs
            if mode == 'baseline':
                assert 'missing-source cleanup returned after reopen: schedules=1 want=0' in outputs
                assert sum(e['Action']=='fail' and bool(e.get('Test')) for e in events)==1
            else:
                assert not any(e['Action']=='fail' for e in events)
            results.append(dict(mode=mode,expected_verdict=verdict,compiled_upstream_changes=sorted(changed),measurement=measured))
        (root/'result.json').write_text(json.dumps(dict(accepted=True,cases=results,scope='Fresh two-message direct file-store reproduction; no server, Raft, copied campaign stores, index removal or production dependency change; original million missed-retirement cause unconfirmed.'),indent=2)+'\n')
    finally:
        after = hashes(module)
        (root/'module-after.json').write_text(json.dumps(after,indent=2)+'\n')
        assert after == upstream
    print((root/'result.json').read_text())

if __name__ == '__main__':
    main()
