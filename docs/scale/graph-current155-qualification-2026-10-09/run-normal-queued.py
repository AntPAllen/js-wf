"""Qualify frozen current155 once one observed campaign service finishes."""
from pathlib import Path
import subprocess
import datetime
import json
import hashlib
import time

base=Path(__file__).resolve().parent
checkout=Path('/home/exedev/js-wf-current155-qualification')
root=Path('/home/exedev/js-wf-tier1-full155-normal1000-20261009')
manifest=json.loads(Path('/home/exedev/js-wf-current155-normal-manifest-20261009/source-frozen.json').read_text())
units=['js-wf-full151-race-20261009.service','js-wf-focused-race-20261009.service']

def stamp(): return datetime.datetime.now(datetime.timezone.utc).isoformat()
def observed(unit):
    output=subprocess.check_output(['systemctl','--user','show',unit,'-p','ActiveState','-p','SubState','-p','MainPID','-p','InvocationID'],text=True)
    state=dict(line.split('=',1) for line in output.splitlines())
    pid=int(state['MainPID'])
    if pid:
        proc=Path('/proc')/str(pid)
        try:
            fields=(proc/'stat').read_text().split(') ',1)[1].split()
            state['process']={'pid':pid,'state':fields[0],'start_ticks':fields[19],'arguments':[x.decode() for x in (proc/'cmdline').read_bytes().split(b'\0') if x]}
        except FileNotFoundError: state['process']=None
    return state

initial={unit:observed(unit) for unit in units}
assert all(s['ActiveState']=='active' and s.get('process') and s['process']['state']!='Z' for s in initial.values()),initial
assert not root.exists()
record={'source':manifest['revision'],'frozen_inputs':len(manifest['files']),'queued_at':stamp(),'observed_services':initial,'root':str(root),'working_directory':str(checkout),'scope':'complete compiled155 inventory x1000, all835 pins and ordinary groups; normal only; acceptance pending actual result'}
def save(): (base/'normal-launch.json').write_text(json.dumps(record,indent=2)+'\n')
save()
while True:
    current={unit:observed(unit) for unit in units}
    # Count the whole focused pipeline as occupied during actor/pin/expiry
    # transitions. A missing tool handle alone never frees this capacity.
    if sum(s['ActiveState'] in ('active','activating','deactivating') for s in current.values())<2:
        record['capacity_available_at']=stamp();record['services_at_start']=current;save();break
    time.sleep(5)

assert subprocess.check_output(['git','rev-parse','HEAD'],cwd=checkout,text=True).strip()==manifest['revision']
assert not subprocess.check_output(['git','status','--porcelain'],cwd=checkout)
for name,digest in manifest['files'].items(): assert hashlib.sha256((checkout/name).read_bytes()).hexdigest()==digest,name
command=['python3','scripts/check-tier1-race.py','--root',str(root),'--seeds','1000','--no-race']
record.update(started_at=stamp(),command=command);save()
with (base/'supervisor.log').open('w') as out:
    child=subprocess.run(command,cwd=checkout,stdout=out,stderr=subprocess.STDOUT)
record.update(actual_exit_code=child.returncode,finished_at=stamp());save()
child.check_returncode()
result=json.loads((root/'tier1-result.json').read_text())
assert result['pinned_regressions_pass']==835
assert result['per_workload_seed_proof']['workloads']==155
assert result['per_workload_seed_proof']['completed_bodies']==155000
record['runner_verified_complete_normal']=True;save()
