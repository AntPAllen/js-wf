#!/usr/bin/env python3
"""Execute production release-fencing models and a precise compiled omission control."""
import argparse,hashlib,json,os,subprocess
from pathlib import Path
REPO=Path(__file__).resolve().parents[1]
TEST='TestSeededWorkerReleaseFencingReplay'
def main():
 p=argparse.ArgumentParser(description=__doc__);p.add_argument('--root',required=True,type=Path);a=p.parse_args();root=a.root.resolve()
 assert not root.exists() and not root.is_relative_to(REPO)
 assert not subprocess.check_output(['git','status','--porcelain'],cwd=REPO)
 names=subprocess.check_output(['git','ls-files'],cwd=REPO,text=True).splitlines()
 def hashes():return {n:hashlib.sha256((REPO/n).read_bytes()).hexdigest() for n in names if n.endswith('.go') or n in ('go.mod','go.sum','scripts/check-worker-release-fencing.py','.github/workflows/worker-release-fencing.yml') or n.startswith('sim/testdata/regressions/')}
 root.mkdir(parents=True);before=hashes();(root/'source.json').write_text(json.dumps(dict(revision=subprocess.check_output(['git','rev-parse','HEAD'],cwd=REPO,text=True).strip(),files=before),indent=2)+'\n')
 source=REPO/'worker/worker.go';text=source.read_text();needle='recordFencing("lease_release_lost", err)';assert text.count(needle)==1
 (root/'control.go').write_text(text.replace(needle,'// compiled control: omit first failed release ownership observation'))
 overlay=root/'overlay.json';overlay.write_text(json.dumps({'Replace':{str(source):str(root/'control.go')}})+'\n')
 try:
  for mode in ('positive','negative'):
   cmd=['go','test','-p=1','-race','-json']
   if mode=='negative':cmd+=['-overlay='+str(overlay)]
   expression='^Test(SeededWorkerReleaseFencingReplay|SeededHeartbeatHandoffReplay|PinnedRegressionCorpus)$' if mode=='positive' else '^'+TEST+'$'
   cmd+=['./sim','-run',expression,'-count=1','-timeout=8m']
   (root/(mode+'-command.json')).write_text(json.dumps(cmd,indent=2)+'\n')
   env=dict(os.environ,GOMEMLIMIT='512MiB',GOMAXPROCS='2',SIM_SEEDS='1000',SIM_COVERAGE_SUMMARY='1',FAULT_TRACE_OUT=str(root/(mode+'-trace.json')))
   with (root/(mode+'-events.jsonl')).open('w') as out,(root/(mode+'-stderr.log')).open('w') as err:result=subprocess.run(cmd,cwd=REPO,env=env,stdout=out,stderr=err)
   events=[json.loads(l) for l in (root/(mode+'-events.jsonl')).read_text().splitlines()];output=''.join(e.get('Output','') for e in events)
   assert not any(e['Action'] in ('build-fail','skip') for e in events)
   named=[e['Action'] for e in events if e.get('Test')==TEST and e['Action'] in ('pass','fail')];package=[e['Action'] for e in events if not e.get('Test') and e['Action'] in ('pass','fail')]
   if mode=='positive':assert result.returncode==0 and named==['pass'] and package==['pass']
   else:
    assert result.returncode!=0 and named==['fail'] and package==['fail']
    assert 'lost release accounting mode=retry_missing counter=0 events=0 want=1' in output
    assert 'panic: test timed out' not in output
    trace=json.loads((root/'negative-trace.json').read_text());assert trace['workload']=='worker_release_fencing'
  (root/'result.json').write_text(json.dumps(dict(actual_race_models_pass=True,actual_release_omission_control_detected=True,scope='Focused worker release-fencing, heartbeat handoff and pinned models; not full121 suite or real R5 qualification.'),indent=2)+'\n')
 finally:
  after=hashes();(root/'source-after.json').write_text(json.dumps(after,indent=2)+'\n');assert after==before
if __name__=='__main__':main()
