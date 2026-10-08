import hashlib,json,pathlib,re,subprocess
base=pathlib.Path('docs/scale/graph-signal-runtime-tier1-2026-10-08/qualification')
source='dd98e39c29c5e8689f06fdbdcca7e944b53c0963'
before=json.loads((base/'race-source-before.json').read_text())
after=json.loads((base/'race-source-after.json').read_text())
assert before==after and before['source']==source
frozen=pathlib.Path('/home/exedev/js-wf-signal-runtime-qualification')
for p,digest in before['inputs'].items():
 assert hashlib.sha256((frozen/p).read_bytes()).hexdigest()==digest
 assert hashlib.sha256(subprocess.check_output(['git','show',source+':'+p])).hexdigest()==digest
commands=json.loads((base/'race-commands.json').read_text())['commands']
assert len(commands)==1 and commands[0]['exit_code']==1 and '-race' in commands[0]['command']
rows=[json.loads(l) for l in (base/'race-family.jsonl').read_text().splitlines()]
out=''.join(r.get('Output','') for r in rows)
assert 'panic: test timed out after 20m0s' in out
assert 'runGraphSignalRuntime(0x2ed, 0xc' in out # seed 749, replay argument nonnil
assert 'encoding/json/v2.marshalEncode' in out
assert not any(r.get('Action')=='pass' and 'Test' not in r for r in rows)
assert 'WARNING: DATA RACE' not in out
result={'source':source,'selected_inputs':len(before['inputs']),'verdict':'FAIL','reason':'20-minute test alarm while replaying seed 749','observed_stack':'ordinary graph reader release/root validation JSON encoding','package_seconds':1200.034,'assertion_failure_or_data_race_reported':False,'source_unchanged':True,'next_action':'preserve failure; rely on already-running complete race suite for qualification, avoid duplicate focused run; correct future CI CPU timeout and add opt-in seed checkpoints','scope':'This failure does not qualify all 1000 seeds or prove absence of a later bug.'}
(base/'timeout-review.json').write_text(json.dumps(result,indent=2)+'\n')
print(json.dumps(result))
