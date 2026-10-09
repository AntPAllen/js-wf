import hashlib,json,re
from pathlib import Path
base=Path(__file__).resolve().parent;repo=base.parents[2]
before=json.loads((base/'source-observed.json').read_text())
for p,h in before['files'].items():assert hashlib.sha256((repo/p).read_bytes()).hexdigest()==h,p
assert json.loads((base/'baseline-cohort.exit.json').read_text())['exit_code']==1
baseline=(base/'baseline-cohort.log').read_text()
assert 'stable readers=32 failed=14 acknowledged_physical_witnesses=18 logical_head=0' in baseline
assert 'blob publication CAS conflict' in baseline
p=base/'package-race.log';s=p.read_text()
assert json.loads((base/'package-race.exit.json').read_text())['exit_code']==0
assert re.search(r'^ok\s+js-wf/internal/graphpublication\s+[\d.]+s$',s,re.M)
assert not re.search(r'^\s*--- FAIL:|^FAIL(?:\s|$)|WARNING: DATA RACE|^panic:',s,re.M)
for name in ['TestNativeGraphStableReadCohort','TestNativeGraphQueuedReadCancellationAndConcurrentMutation','TestNativeGraphAuthorityReadWitness','TestNativeGraphAuthorityLostMutationReplies','TestNativeGraphRuntimePermissions','TestNativeGraphConcurrentPublishersAndCollectors','TestNativeGraphReadersRetirementAndExpiry','TestNativeGraphReaderCheckpointStoreRestart']:
 assert re.search(r'^--- PASS: '+name+r' \(',s,re.M),name
assert s.count('stable readers=32 failed=0 acknowledged_physical_witnesses=32 logical_head=0')==2
logs={'package_race':{'sha256':hashlib.sha256(p.read_bytes()).hexdigest(),'top_groups_pass':len(re.findall(r'^--- PASS:',s,re.M))}}
for kind in ['normal','race']:
 p=base/('worker-'+kind+'.log');s=p.read_text();meta=json.loads((base/('worker-'+kind+'.exit.json')).read_text())
 assert meta['exit_code']==0 and meta['environment']=={'GOMAXPROCS':'2','GOMEMLIMIT':'512MiB'}
 assert re.search(r'^ok\s+js-wf/cmd/wf-worker\s+[\d.]+s$',s,re.M)
 assert not re.search(r'^\s*--- FAIL:|^FAIL(?:\s|$)|WARNING: DATA RACE|^panic:',s,re.M)
 for version in [4,5,6]:
  for rep in ['R1','R3Domain']:
   test='TestWorkerRunnerCanonicalGraphRepair/'+rep if version==4 else f'TestWorkerRunnerCanonicalCheckpointGraphRepair/v{version}/{rep}'
   assert re.search(r'^\s*--- PASS: '+re.escape(test)+r' \(',s,re.M),(kind,test)
 logs[kind]={'sha256':hashlib.sha256(p.read_bytes()).hexdigest(),'time':re.findall(r'^ok\s+js-wf/cmd/wf-worker\s+([\d.]+s)$',s,re.M)}
review={'verdict':'PASS complete graph authority package race and six worker CLI cases normal/race at two Go CPUs','repository_inputs_unchanged':len(before['files']),'logs':logs,'scope':'Native read coordination only. Default-four-CPU combined CLI race qualification remains pending separately; original failure cause is not attributed exclusively to the gate. Full current/extended and all broader plan gates remain open.'}
(base/'review.json').write_text(json.dumps(review,indent=2)+'\n');print(json.dumps(review))
