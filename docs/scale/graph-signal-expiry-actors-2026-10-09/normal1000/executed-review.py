from pathlib import Path
import json,hashlib,re,subprocess
base=Path(__file__).resolve().parent
repo=Path('/home/exedev/js-wf')
root=Path('/home/exedev/js-wf-signal-expiry-actors-20261009')
source='e971d01'
observed=json.loads((root/'observed-source.json').read_text())
for name,digest in observed['files'].items():
 assert hashlib.sha256(subprocess.check_output(['git','show',f'{source}:{name}'],cwd=repo)).hexdigest()==digest,name
 assert hashlib.sha256((repo/name).read_bytes()).hexdigest()==digest,name
binary=json.loads((root/'normal-binary.json').read_text());assert '-race=true' not in binary['build_info']
assert hashlib.sha256((root/'normal.test').read_bytes()).hexdigest()==binary['sha256']
exit_record=json.loads((root/'normal-exit.json').read_text());assert exit_record['exit_code']==0
log=(root/'normal1000.log').read_text()
assert 'TIER1_SEEDS test=TestSeededGraphSignalExpiryActorsReplay first=1 last=1000 completed=1000 requested=1000' in log
assert '--- PASS: TestSeededGraphSignalExpiryActorsReplay (597.57s)' in log
assert not re.search(r'^--- FAIL:|WARNING: DATA RACE|panic:',log,re.M)
coverage=re.search(r'combinations: map\[([^\]]+)\]',log).group(1)
counts={key:int(count) for key,count in re.findall(r'([^ ]+):(\d+)',coverage)}
assert len(counts)==30 and sum(counts.values())==1000
captures={}
for p in sorted((root/'pins').glob('*.json')):
 d=json.loads(p.read_text());assert d['workload']=='graph_signal_expiry_actors'
 cut=[e for e in d['transport'] if e['operation']=='graph_publication_intent_expiry_cut']
 assert bool(cut)==(d['decisions'][1]['chosen']=='expire_intents')
 assert d['decisions'][1]['chosen']+'-'+d['decisions'][2]['chosen']+'.json'==p.name
 captures[p.name]={'seed':d['seed'],'sha256':hashlib.sha256(p.read_bytes()).hexdigest(),'intent_expiry_cuts':len(cut),'choices':len(d['decisions'])}
assert len(captures)==6
result={'source_go_and_original_trace_byte_matched_to':source,'source_inputs':len(observed['files']),'completed_bodies':1000,'exact_replays':1000,'elapsed_seconds':597.57,'combinations':counts,'captures':captures,'source_snapshot_limitation':'captured after launch during live jobs, excludes subsequently added five runtime regression files','race_acceptance':'pending; first attempt intentionally stopped for CPU serialization','scope':'focused new family only; current whole-suite/all-pin/extended and broader original gates remain open'}
(base/'review-result.json').write_text(json.dumps(result,indent=2)+'\n')
print(json.dumps(result))
