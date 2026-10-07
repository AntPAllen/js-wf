from pathlib import Path
import sys,importlib.util,json,subprocess,shutil,hashlib
sys.dont_write_bytecode=True
repo=Path('/home/exedev/js-wf');sys.path.insert(0,str(repo/'scripts'));import fixture_archive
spec=importlib.util.spec_from_file_location('shared',repo/'scripts/run-domain-runtime-controls.py');shared=importlib.util.module_from_spec(spec);spec.loader.exec_module(shared)
root=Path('/tmp/js-wf-checkpoint8520-watch-peer-outage-20261007');revision=json.loads((root/'source-before.json').read_text())['revision']
service='js-wf-checkpoint8520-watch-peer-outage-20261007.service'
state=subprocess.check_output(['systemctl','show',service,'-p','ActiveState','-p','SubState','-p','MainPID','-p','ExecMainStatus','-p','Restart','-p','KillMode'],text=True)
assert 'MainPID=0\n' in state and 'ExecMainStatus=1\n' in state and 'Restart=no\n' in state and 'KillMode=control-group\n' in state
journal=subprocess.check_output(['journalctl','-u',service,'--no-pager'],text=True)
assert "KeyError: b'GOMAXPROCS'" in journal
(root/'producer-journal.log').write_text(journal)
(root/'unadmitted-producer.json').write_text(json.dumps(dict(source=revision,service=service,service_state=state,qualified=False,native_outcome='unadmitted SDK identity capture failure; no completed native outcome',failure="first /proc environment lacked GOMAXPROCS",scope='Preserve original failed collection without inferring SDK outcome or restarting stores.'),indent=2)+'\n')
after=shared.source_inventory(revision);assert after==json.loads((root/'source-before.json').read_text())
(root/'source-after.json').write_text(json.dumps(after,indent=2)+'\n')
external=json.loads((root/'external-source-before.json').read_text());assert external=={n:shared.sha(Path(n)) for n in external}
(root/'external-source-after.json').write_text(json.dumps(external,indent=2)+'\n')
reference=json.loads((root/'donor-reference.json').read_text());donor=Path(reference['root']);canonical=reference['canonical']
inventory=json.loads(subprocess.check_output(['git','cat-file','blob',revision+':'+canonical+'/fixture-inventory.json'],cwd=repo))
assert fixture_archive.inventory(donor)==inventory['files']
(root/'original-after-verification.json').write_text(json.dumps(dict(all_original_bytes_modes_mtimes_unchanged=True,original_files=len(inventory['files']),visible_closure=shared.closure(donor)),indent=2)+'\n')
(root/'closure.json').write_text(json.dumps(shared.closure(root),indent=2)+'\n')
shutil.copyfile(__file__,root/'executed-preservation.py')
print(json.dumps(fixture_archive.capture(root,root.with_suffix('.tar.gz'),root.with_name(root.name+'-proof'),compresslevel=1)),flush=True)
