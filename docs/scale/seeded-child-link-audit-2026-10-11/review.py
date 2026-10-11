import hashlib,json,re
from pathlib import Path
import yaml
out=Path(__file__).resolve().parent
manifest=json.loads((out/'source-inputs.json').read_text())
for name,sha in manifest['files'].items():assert hashlib.sha256(Path(name).read_bytes()).hexdigest()==sha,name
workflow=yaml.safe_load(Path('.github/workflows/graph-publication.yml').read_text())
script=next(step['run'] for step in workflow['jobs']['seeded-child-link-audit']['steps'] if 'run' in step)
guard=script.split("python3 - <<'PY'\n",1)[1].rsplit('\nPY',1)[0]
modes={}
for mode in ('normal','race'):
 command=json.loads((out/(mode+'-command.json')).read_text());assert command['actual_exit_code']==0 and command['executable_captured']
 executable=json.loads((out/(mode+'-executable.json')).read_text());assert ('-race=true' in executable['build'])==(mode=='race')
 log=(out/(mode+'.log')).read_text();code=guard.replace("pathlib.Path('child-link-audit.log').read_text()",'provided_log');exec(code,{'provided_log':log})
 assert not re.search(r'^--- FAIL:',log,re.M)
 modes[mode]={'actual_exit':0,'test_seconds':float(re.search(r'ok\s+js-wf/sim\s+([\d.]+)s',log).group(1)),'requested_and_completed_bodies':1000,'exact_pinned_replays':28,'shared_failure_persistence_controls':2,'actual_executable_sha256':executable['sha256'],'exact_ci_guard_executed':True}
assert len((out/'seed-inventory.txt').read_text().splitlines())==162
pins=sorted(Path('sim/testdata/regressions').glob('graph_child_link_audit_*.json'));assert len(pins)==28
for pin in pins:
 trace=json.loads(pin.read_text());assert trace['workload']=='graph_child_link_audit'
mutations={}
for mode in ('start','terminal'):
    source=json.loads((out/(mode+'-mutation-source.json')).read_text())
    original=Path(source['source']).read_text()
    changed=(out/(mode+'-guard-disabled.go.txt')).read_text()
    assert original.count(source['anchor'])==1 and changed==original.replace(source['anchor'],source['replacement'])
    assert hashlib.sha256(original.encode()).hexdigest()==source['original_sha256']
    assert hashlib.sha256(changed.encode()).hexdigest()==source['changed_sha256']
    receipt=json.loads((out/(mode+'-mutation-command.json')).read_text())
    assert receipt['actual_exit']!=0 and receipt['semantic_failure']
    log=(out/(mode+'-mutation.log')).read_text()
    assert re.search(r'FAULT_SEED=\d+.*child links (consumed|rejected)/(parent_mismatch|input_mismatch|projection_mismatch): <nil>',log)
    minimized=json.loads((out/(mode+'-minimize-command.json')).read_text());assert minimized['actual_exit']==0
    assert '--- PASS: TestMinimizeFaultTrace ' in (out/(mode+'-minimize.log')).read_text()
    mutations[mode]={'semantic_detection':True,'exact_failure_replayed_and_minimized':True,'executables_captured':False}
result={'audit_guard_controls':mutations,'new_family':modes,'unchanged_inputs':len(manifest['files']),'families':162,'total_corpus_files':len(list(Path('sim/testdata/regressions').glob('*.json'))),'scope':'New family only; synthetic retained histories/test-only metadata corruption, not worker interleaving or NATS conformance, full current/extended qualification or power durability'}
(out/'review.json').write_text(json.dumps(result,indent=2)+'\n');print(json.dumps(result))
