from pathlib import Path
import subprocess,json,sys
repo=Path('/home/exedev/js-wf');root=Path('/tmp/js-wf-partition-generator-sparse-control-20261006');root.mkdir()
checkout=root/'checkout'
subprocess.run(['git','clone','--quiet','--shared','--no-checkout',str(repo),str(checkout)],check=True)
subprocess.run(['git','-C',str(checkout),'sparse-checkout','set','.github','scripts'],check=True)
subprocess.run(['git','-C',str(checkout),'checkout','--quiet','fcf2e940ee22f2dda7116d1d860683bcaa3bc329'],check=True)
cmd=['python3','-m','unittest','discover','-s','scripts','-p','test_matrix_*.py']
r=subprocess.run(cmd,cwd=checkout,capture_output=True,text=True)
(root/'baseline.stdout').write_text(r.stdout);(root/'baseline.stderr').write_text(r.stderr)
assert r.returncode!=0 and "No such file or directory: 'integration/mixed_matrix_leader_test.go'" in r.stderr
subprocess.run(['git','-C',str(checkout),'sparse-checkout','add','integration'],check=True)
r=subprocess.run(cmd,cwd=checkout,capture_output=True,text=True)
(root/'control.stdout').write_text(r.stdout);(root/'control.stderr').write_text(r.stderr)
assert r.returncode==0 and 'Ran 24 tests' in r.stderr
r=subprocess.run(['python3','-m','unittest','discover','-s','scripts','-p','test_workload_source.py'],cwd=checkout,capture_output=True,text=True)
(root/'workload-source.stdout').write_text(r.stdout);(root/'workload-source.stderr').write_text(r.stderr);assert r.returncode==0
(root/'validation.json').write_text(json.dumps(dict(accepted=True,source='fcf2e940ee22f2dda7116d1d860683bcaa3bc329',original_sparse_paths=['.github','scripts'],control_sparse_paths=['.github','scripts','integration'],baseline='exact hosted missing integration source failure reproduced',control='24 matrix and 1 workload-source tests pass',runtime_tests_executed=False,scope='Generator source materialization correction only; no fault/duration/history/latency/drain gate changed.'),indent=2)+'\n')
print('SPARSE_CHECKOUT_BASELINE_FAIL_CONTROL_PASS',flush=True)
