import pathlib,json,hashlib,subprocess
repo=pathlib.Path('/home/exedev/js-wf');out=pathlib.Path(__file__).parent
previous=repo/'docs/scale/current-tier3-clock-2026-10-04/worker-clock-170-182/partial-row-coverage.json'
old=json.loads(previous.read_text());source=old['source'];summaries=[];seeds=set()
for item in old['summaries']:
 p=repo/item['path'];data=p.read_bytes();assert hashlib.sha256(data).hexdigest()==item['sha256']
 assert subprocess.check_output(['git','show','8b69a31eed72c5973af144b802fdea19a3bdcd88':'+item['path']],cwd=repo)==data
 assert json.loads(data)==item['summary'];summaries.append(item)
p=out/'summary.json';data=p.read_bytes();summaries.append(dict(path=str(p.relative_to(repo)),sha256=hashlib.sha256(data).hexdigest(),summary=json.loads(data)))
for item in summaries:
 s=item['summary'];assert s['source']==source and s['all_three_models_pass_every_seed'] and not s['qualifies_full_row']
 covered=set(range(s['first_seed'],s['last_seed']+1));assert not seeds&covered;seeds|=covered
assert len(seeds)==122 and seeds==set(range(27,53))|set(range(66,105))|set(range(131,183))|set(range(196,201))
report=dict(source=source,accepted_seeds=sorted(seeds),accepted_seed_count=len(seeds),invocations=sum(i['summary']['invocations'] for i in summaries),journal_entries=sum(i['summary']['journal_entries'] for i in summaries),recorded_faults=sum(i['summary']['confirmed_faults'] for i in summaries),history_operations=sum(i['summary']['independent_history_operations'] for i in summaries),summaries=summaries,qualifies_full_row=False,qualifies_failed_parent=False,qualifies_current_main_full_matrix=False,qualifies_actual24h=False,scope='Coverage calculation over separately accepted same-source proofs; not a rerun or promotion of historical failed seeds.')
(out/'partial-row-coverage.json').write_text(json.dumps(report,indent=2)+'\n');print(json.dumps({k:v for k,v in report.items() if k not in ['summaries','accepted_seeds']},indent=2))
