import sys,json,re,hashlib,subprocess,datetime
from pathlib import Path
sys.dont_write_bytecode=True
repo=Path('/home/exedev/js-wf');b=repo/'docs/scale/graph-reader-recovery-2026-10-08'
before=json.loads((b/'source-before.json').read_text());after=json.loads((b/'source-after.json').read_text())
assert before['selected_inputs_match_git'] and after['unchanged'] and before['files']==after['files']
assert all(hashlib.sha256((repo/n).read_bytes()).hexdigest()==h for n,h in before['files'].items())
results=json.loads((b/'results.json').read_text());assert results['source']==before['revision']
expected_graph=set()
for p in (repo/'internal/graphpublication').glob('*_test.go'):
    expected_graph.update(re.findall(r'^func (Test\w+)\(t \*testing.T\)',p.read_text(),re.M))
assert len(expected_graph)==42
reports=[]
for result in results['runs']:
    assert result['exit_code']==0
    rows=[json.loads(l) for l in (b/(result['name']+'.jsonl')).read_text().splitlines()]
    assert not any(r.get('Action')=='fail' for r in rows)
    tops={r['Test'] for r in rows if r.get('Action')=='pass' and r.get('Test') and '/' not in r['Test'] and r['Package']=='js-wf/internal/graphpublication'}
    item=dict(name=result['name'],package_passes=result['package_passes'])
    if result['name'].startswith('native'):
        assert tops==expected_graph
        native={r['Test'] for r in rows if r.get('Action')=='pass' and r.get('Test','').startswith('TestNativeGraphReaderCheckpointStoreRestart/')}
        assert native=={'TestNativeGraphReaderCheckpointStoreRestart/R1','TestNativeGraphReaderCheckpointStoreRestart/R3'}
        item.update(graph_top_level_groups=len(tops),reader_native_replicas=[1,3])
    else:
        logs=[r['Output'] for r in rows if 'reader recovery: modes=map[' in r.get('Output','')]
        assert len(logs)==1
        counts={k:int(v) for k,v in re.findall(r'(\w+):(\d+)',logs[0].split('modes=map[')[1].split(']')[0])}
        assert len(counts)==9 and sum(counts.values())==int(result['environment']['SIM_SEEDS'])
        pins={r['Test'].split('/',1)[1] for r in rows if r.get('Action')=='pass' and r.get('Test','').startswith('TestPinnedRegressionCorpus/graph-')}
        expected_pins={p.name for pattern in ('graph-publication-*.json','graph-readers-*.json','graph-reader-resume-*.json') for p in (repo/'sim/testdata/regressions').glob(pattern)}
        assert pins==expected_pins and len(pins)==38
        item.update(modes=counts,total_schedules=sum(counts.values()),exact_graph_pins=len(pins))
    reports.append(item)
report=dict(source=before['revision'],selected_source_inputs=len(before['files']),selected_inputs_match_git_before=True,selected_inputs_unchanged_after=True,verified_runs=reports,go_version=subprocess.check_output(['go','version'],text=True).strip(),utc=datetime.datetime.now(datetime.timezone.utc).isoformat(),scope='Standard local package and focused seeded regression only; not independent SDK executable/peer-media provenance, full current-source Tier1 qualification, deployment rollout or complete native release qualification.',original_full126_observation=subprocess.check_output(['ps','-p','2827904','-o','pid,etime,state,args'],text=True))
(b/'review.json').write_text(json.dumps(report,indent=2)+'\n')
(b/'executed-review.py').write_bytes(Path(__file__).read_bytes())
print(json.dumps(dict(graph_groups=len(expected_graph),runs=len(reports),source_inputs=len(before['files']))))
