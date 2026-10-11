from pathlib import Path
import subprocess, json, hashlib, datetime
base = Path(__file__).resolve().parent
repo = base.parents[2]
root = Path('/home/exedev/js-wf-raw-promise-audit-20261011')
root.mkdir(exist_ok=True)
files = list((repo/'integrity').glob('graph*.go'))+list((repo/'internal/checkpoint').glob('*.go'))+[repo/'integrity/streaming_journal.go',repo/'go.mod',repo/'go.sum',Path(__file__).resolve()]
def hashes(): return {str(p.relative_to(repo)):hashlib.sha256(p.read_bytes()).hexdigest() for p in files}
before = hashes()
source = subprocess.check_output(['git','rev-parse','HEAD'],cwd=repo,text=True).strip()
(base/'source.json').write_text(json.dumps(dict(base=source,files=before,scope='Focused development-worktree race only'),indent=2)+'\n')
binary = root/'integrity-race.test'
compile_command = ['go','test','-race','-c','-o',str(binary),'./integrity']
subprocess.run(compile_command,cwd=repo,check=True)
selector = '^(TestRawGraphReferencesAndIndependentCorruptionControls|TestNativeRawGraphReferenceAuditQuiescentReaders|TestRawGraphJournalGenerationsOutcomesAndArchive|TestRawGraphCheckpointPointerAndFrameBindings|TestNativeRawGraphCheckpointAudit|TestRawGraphCheckpointMaterializedState|TestRawGraphCheckpointPromiseOwnership)$'
command = [str(binary),'-test.run='+selector,'-test.count=1','-test.v','-test.timeout=5m']
started = datetime.datetime.now(datetime.timezone.utc).isoformat()
with (base/'race.log').open('wb') as output:
    child = subprocess.Popen(command,cwd=repo,stdout=output,stderr=subprocess.STDOUT)
    receipt = dict(binary_path=str(binary),binary_sha256=hashlib.sha256(binary.read_bytes()).hexdigest(),build_info=subprocess.check_output(['go','version','-m',str(binary)],text=True),compile_command=compile_command,command=command,actual_child_pid=child.pid,started=started)
    (base/'binary.json').write_text(json.dumps(receipt,indent=2)+'\n')
    exit_code = child.wait()
state = dict(actual_child_exit=exit_code,source_inputs_unchanged=before==hashes(),finished=datetime.datetime.now(datetime.timezone.utc).isoformat())
(base/'state.json').write_text(json.dumps(state,indent=2)+'\n')
print(json.dumps(state,indent=2))
raise SystemExit(exit_code)
