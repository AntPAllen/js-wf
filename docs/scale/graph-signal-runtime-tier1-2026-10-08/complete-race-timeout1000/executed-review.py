import hashlib,json,pathlib,re,subprocess
root=pathlib.Path.cwd()
base=root/'docs/scale/graph-signal-runtime-tier1-2026-10-08/complete-race-timeout1000'
source='dd98e39c29c5e8689f06fdbdcca7e944b53c0963'
source_root=pathlib.Path('/home/exedev/js-wf-signal-runtime-qualification')
before=json.loads((base/'source-before.json').read_text());after=json.loads((base/'source-after.json').read_text())
assert before==after and before['revision']==source
for path,digest in before['files'].items():
 assert hashlib.sha256(subprocess.check_output(['git','show',source+':'+path])).hexdigest()==digest,path
 assert hashlib.sha256((source_root/path).read_bytes()).hexdigest()==digest,path
binary=json.loads((base/'binary.json').read_text())
assert binary['race_instrumented'] and '-race=true' in binary['build_info']
assert hashlib.sha256(pathlib.Path('/home/exedev/js-wf-tier1-full145-race1000-20261008/sim.test').read_bytes()).hexdigest()==binary['binary_sha256']
data=(base/'tier1-events.jsonl').read_bytes();rows=[json.loads(s) for s in data.splitlines()]
assert rows[-1]['Action']=='fail' and rows[-1]['Test']=='TestSeededSuspendedScanCapacityReplay'
output=''.join(r.get('Output','') for r in rows)
assert 'panic: test timed out after 1h0m0s' in output
assert 'WARNING: DATA RACE' not in output
commands=json.loads((base/'commands.json').read_text())
assert any('-test.timeout=60m' in c for c in commands)
assert not any('scripts/check-tier1-suite.py' in c for c in commands)
passes={r['Test'] for r in rows if r['Action']=='pass' and r.get('Test')}
assert 'TestSeededGraphSignalRuntimeReplay' in passes and 'TestPinnedRegressionCorpus' in passes
assert 'TIER1_SEEDS test=TestSeededGraphSignalRuntimeReplay first=1 last=1000 completed=1000 requested=1000' in output
pins={n.split('/')[-1] for n in before['files'] if n.startswith('sim/testdata/regressions/') and n.endswith('.json')}
assert len(pins)==728 and {s.split('/',1)[1] for s in passes if s.startswith('TestPinnedRegressionCorpus/')}==pins
seed_proofs=re.findall(r'TIER1_SEEDS test=(\w+) first=1 last=1000 completed=1000 requested=1000',output)
assert len(set(seed_proofs))==len(seed_proofs)
report=dict(source=source,selected_inputs=len(before['files']),source_and_binary_unchanged=True,events_sha256=hashlib.sha256(data).hexdigest(),verdict='FAIL',cause='60-minute test alarm',interrupted_test=rows[-1]['Test'],complete_seeded_families=len(seed_proofs),passed_groups=len([s for s in passes if '/' not in s]),signal_runtime_1000_seed_group='PASS at frozen source',pins=728,data_race_report_observed=False,scope='Complete default race campaign failed and did not qualify the suite. Individually passed Signal runtime group and pins retain their exact frozen-source scope; no current-source or extended qualification inferred.')
(base/'review.json').write_text(json.dumps(report,indent=2)+'\n')
print(json.dumps(report))
