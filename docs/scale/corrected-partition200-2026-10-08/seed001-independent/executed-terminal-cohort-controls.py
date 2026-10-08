import sys,json,importlib.util,hashlib
from pathlib import Path
sys.dont_write_bytecode=True
repo=Path('/home/exedev/js-wf');sys.path.insert(0,str(repo/'scripts'))
spec=importlib.util.spec_from_file_location('review',repo/'scripts/review-local-tier2-partition.py');review=importlib.util.module_from_spec(spec);spec.loader.exec_module(review)
seed=Path('/tmp/js-wf-corrected-partition200-20261008/campaign/seed-001');out=Path('/tmp/js-wf-corrected-partition200-seed001-independent-v2-20261008')
segment=(seed/'native.log').read_text()+'\n'+(seed/'acceptance.log').read_text()
n=json.loads((out/'seed-001.json').read_text())['raw_review']['invocations']
positive=review.terminal_cohort(segment,n,True)
marker=f"MATRIX_TERMINAL_COHORT cutoff={positive['cutoff']} expected_invocations={n} visited_invocations={n}"
changes=[('missing',segment.replace(marker,'')),('duplicate',segment+'\n'+marker),('stale_latency_population',segment.replace(f'visited_invocations={n}',f'visited_invocations={n-28}')),('stale_expected_population',segment.replace(f'expected_invocations={n} visited',f'expected_invocations={n-28} visited')),('stale_physical_cut',segment.replace(f'cutoff={positive["cutoff"]} expected',f'cutoff={n-28} expected')),('missing_final_retained',segment.replace('MATRIX_RETAINED row=server_partition ','')),('invented_partition_log_label',segment.replace('MATRIX_RETAINED row=server_partition ','MATRIX_RETAINED row=partition ')),('pre_final_checkpoint',segment.replace('checkpoint audit batch=60 invocation_cutoff=1680','checkpoint audit batch=61 invocation_cutoff=1708'))]
rejected=[]
for label,changed in changes:
 try:review.terminal_cohort(changed,n,True)
 except ValueError:rejected.append(label)
 else:raise AssertionError(label)
assert (seed/'native.log').read_text()+'\n'+(seed/'acceptance.log').read_text()==segment
(out/'terminal-cohort-controls.json').write_text(json.dumps(dict(positive=positive,raw_log_sha256=hashlib.sha256((seed/'native.log').read_bytes()).hexdigest(),actual_log_mutations_rejected=rejected,actual_original_unchanged=True,scope='Offline log substitutions only. No native SDK/server or fault repeated.'),indent=2)+'\n')
print('ACTUAL_TERMINAL_COHORT_SUBSTITUTIONS_REJECTED',len(rejected))
