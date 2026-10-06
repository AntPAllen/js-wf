from pathlib import Path
import sys,json,subprocess,importlib.util,shutil
repo=Path('/home/exedev/js-wf');sys.path.insert(0,str(repo/'scripts'));import fixture_archive
spec=importlib.util.spec_from_file_location('shared',repo/'scripts/run-domain-runtime-controls.py');shared=importlib.util.module_from_spec(spec);spec.loader.exec_module(shared)
r=Path('/tmp/js-wf-partition-seed6-quiescent-20261006');out=repo/'docs/scale/local-tier2-partition-2026-10-06/seed-006-quiescent-restart/initial-observer-failure'
unit=dict(l.split('=',1) for l in subprocess.check_output(['systemctl','--user','show','js-wf-partition-seed6-quiescent-20261006.service','-p','ActiveState','-p','MainPID','-p','ExecMainStatus'],text=True).splitlines());assert unit==dict(ActiveState='failed',MainPID='0',ExecMainStatus='1')
manifest=json.loads((repo/'docs/scale/local-tier2-partition-2026-10-06/seed-006-failure/fixture-inventory.json').read_text());assert fixture_archive.inventory(r/'restored')==manifest['files']
closure=shared.closure(r)
(r/'executed-producer.py').write_bytes(subprocess.check_output(['git','cat-file','blob','e473884:scripts/run-partition-recovery-diagnostic.py'],cwd=repo))
proof=fixture_archive.capture(r,r.with_suffix('.tar.gz'),out,compresslevel=1)
for name in ['source-before.json','commands.json','restore.json','actual-helper.json','observed-servers.json']:shutil.copyfile(r/name,out/name)
shutil.copyfile(r/'diagnostics'/'result.json',out/'unqualified-observation.json')
(out/'retention.json').write_text(json.dumps(dict(unit=unit,closure=closure,restored_baseline_unchanged=True,qualified=False,scope='Initial diagnostic helper reports quiescent recovery but producer rejects missing live server records: mutable root did not match observer originals scope. No exact executed server claim, native row pass or cause inferred. Full initial attempt preserved; original failed seed and baseline unchanged.'),indent=2)+'\n')
shutil.copyfile(__file__,out/'executed-retention.py');print(json.dumps(proof))
