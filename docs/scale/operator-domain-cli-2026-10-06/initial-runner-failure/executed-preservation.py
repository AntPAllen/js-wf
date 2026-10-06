from pathlib import Path
import sys,importlib.util,subprocess,json,shutil,hashlib
repo=Path('/home/exedev/js-wf');sys.path.insert(0,str(repo/'scripts'));import fixture_archive
spec=importlib.util.spec_from_file_location('native',repo/'scripts/run-domain-runtime-controls.py');m=importlib.util.module_from_spec(spec);spec.loader.exec_module(m)
root=Path('/tmp/js-wf-operator-domain-controls-20261006');b=repo/'docs/scale/operator-domain-cli-2026-10-06/initial-runner-failure';b.mkdir(exist_ok=True)
e=json.loads((root/'execution.json').read_text());assert e['exit_code']==1 and e['source']==subprocess.check_output(['git','rev-parse','026e275'],text=True).strip()
source=json.loads((root/'source-before.json').read_text());assert source==json.loads((root/'source-after.json').read_text())
for n,h in source['files'].items():assert m.sha(root/'selected-source'/n)==h==m.sha(repo/n)==hashlib.sha256(subprocess.check_output(['git','cat-file','blob',e['source']+':'+n])).hexdigest()
log=(root/'native.log').read_text();assert 'testdata/replayplugin: directory not found' in log and log.count('--- FAIL:')==2
sdk=json.loads((root/'actual-sdk.json').read_text());assert not Path('/proc',str(sdk['pid'])).exists();assert sdk['exe_sha256']==m.sha(root/'operator-race.test')
closure=m.closure(root);proof=fixture_archive.capture(root,root.with_suffix('.tar.gz'),b,compresslevel=1)
for n in ['native.log','execution.json','source-before.json','source-after.json','commands.json','actual-sdk.json','binary.json','closure.json']:shutil.copyfile(root/n,b/n)
(b/'independent-preservation.json').write_text(json.dumps(dict(source_files=len(source['files']),source_git_before_after_current_retained_equal=True,actual_sdk_closed=True,execution=e,closure=closure,complete_archive=proof,qualified=False,cause='Compiled SDK launched from repo root; replay plugin build needs cmd/wf package directory. Both named cases failed; no successful CLI suite qualification.'),indent=2)+'\n')
(b/'README.md').write_text('''# Original clean runner failure

Source026e275 built the race SDK but launched it from the repo root. Both operator cases fail when the relative ./testdata/replayplugin path resolves incorrectly. Domain admission and71 traced requests are partial observations only. Plugin absence also caused an early runner assertion before automatic archive capture.

Independent preservation binds exact Git/before/after/current retained inputs, actual closed SDK and complete original stores/source/logs with full archive member readback. Original remains failed. Corrected runner must use cmd/wf package directory and archive failures with missing plugins; SDK3m/body60s/admission30s limits stay unchanged.
''')
shutil.copyfile(__file__,b/'executed-preservation.py')
print(json.dumps(dict(files=len(source['files']),members=proof['members'],bytes=proof['archive_bytes'])))
