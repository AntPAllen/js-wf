"""Capture the terminal, unaccepted old native store without altering its verdict."""
import sys,os,json,gzip,subprocess,hashlib,datetime
from pathlib import Path
base=Path(__file__).resolve().parent
repo=base.parents[2]
sys.path.insert(0,str(repo/'scripts'))
import fixture_archive
root=Path('/home/exedev/js-wf-indexed-entry100000-normal-20261010')
def closure():
    state=json.loads((root/'state.json').read_text())
    props=dict(line.split('=',1) for line in subprocess.check_output(['systemctl','--user','show',state['unit'],'-p','MainPID','-p','LoadState','-p','InvocationID','-p','ExecMainStatus','-p','RemainAfterExit','-p','SubState'],text=True).splitlines())
    assert props['LoadState']=='loaded' and props['MainPID']=='0' and props['SubState']=='exited' and props['ExecMainStatus']=='0' and props['RemainAfterExit']=='yes' and props['InvocationID']==state['invocation']
    assert state['terminal'] and state['child_terminal'] and state['child_exit']==0 and state['supervisor_exit']==0 and not state['accepted']
    assert not Path('/proc',str(state['child_pid'])).exists()
    handles=[]
    for proc in Path('/proc').glob('[0-9]*'):
        for path in [proc/'cwd',proc/'root',proc/'exe',*list((proc/'fd').glob('*'))]:
            try: target=os.readlink(path)
            except (FileNotFoundError,ProcessLookupError):continue
            except PermissionError:continue # Root lsof is checked separately by the operator.
            if target==str(root) or target.startswith(str(root)+'/'):handles.append(str(path))
    assert not handles,handles
    return dict(observed=datetime.datetime.now(datetime.timezone.utc).isoformat(),service=props,child_pid=state['child_pid'],state_sha256=hashlib.sha256((root/'state.json').read_bytes()).hexdigest(),review_sha256=hashlib.sha256((root/'review.json').read_bytes()).hexdigest(),open_handles=handles)
if __name__=='__main__':
    before=closure();(base/'closure-before.json').write_text(json.dumps(before,indent=2)+'\n')
    previous=json.loads(gzip.decompress((root/'native-files.json.gz').read_bytes()))
    current=fixture_archive.inventory(root/'native')
    assert {p:{k:v[k] for k in ('bytes','sha256')} for p,v in current.items()}==previous
    assert len(current)==3170 and sum(v['bytes'] for v in current.values())==3369550584
    archive=Path('/home/exedev/closed-entry100000-native-20261011.tar.gz')
    proof=fixture_archive.capture(root/'native',archive,base,compresslevel=1)
    after=closure();assert before['state_sha256']==after['state_sha256'] and before['review_sha256']==after['review_sha256']
    (base/'closure-after.json').write_text(json.dumps(after,indent=2)+'\n')
    print(json.dumps(proof,indent=2),flush=True)
