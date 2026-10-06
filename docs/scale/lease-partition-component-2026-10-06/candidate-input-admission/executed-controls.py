from pathlib import Path
import sys,subprocess,json,hashlib,shutil
sys.dont_write_bytecode=True
repo=Path('/home/exedev/js-wf');sys.path.insert(0,str(repo/'scripts'));from tier2_retained_profiles import capture_partition_candidate
revision=subprocess.check_output(['git','rev-parse','HEAD'],cwd=repo,text=True).strip()
source=Path('/tmp/js-wf-lease-partition-component-candidate-20261006/candidate-server');canonical=Path('docs/scale/lease-partition-component-2026-10-06/candidate-component')
root=Path('/tmp/js-wf-partition-candidate-input-controls-20261006');root.mkdir();good=root/'good';good.mkdir();bad=root/'bad';bad.mkdir()
expected=hashlib.file_digest(source.open('rb'),'sha256').hexdigest();target=capture_partition_candidate(source,canonical,good,repo,revision);assert hashlib.file_digest(target.open('rb'),'sha256').hexdigest()==expected
mutant=root/'mutated-server';shutil.copy2(source,mutant)
with mutant.open('r+b') as f:f.seek(-1,2);value=f.read(1);f.seek(-1,2);f.write(bytes([value[0]^1]))
try:capture_partition_candidate(mutant,canonical,bad,repo,revision)
except ValueError as error:assert 'differs from source-bound component' in str(error);negative=str(error)
else:raise AssertionError('mutated executable accepted')
profile=(repo/'scripts/tier2_retained_profiles.py').read_bytes();assert profile==subprocess.check_output(['git','cat-file','blob',revision+':scripts/tier2_retained_profiles.py'],cwd=repo)
(root/'result.json').write_text(json.dumps({'source':revision,'profile_sha256':hashlib.sha256(profile).hexdigest(),'valid_source_bound_input_accepted':True,'one_byte_mutated_executable_rejected':True,'rejection':negative,'expected_sha256':expected,'scope':'Focused executable binding admission controls only; no native workflow execution or matrix qualification.'},indent=2)+'\n')
shutil.copyfile(__file__,root/'executed-controls.py');print((root/'result.json').read_text())
