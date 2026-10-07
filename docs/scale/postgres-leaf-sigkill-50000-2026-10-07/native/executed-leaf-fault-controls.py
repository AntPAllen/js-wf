#!/usr/bin/env python3
"""Reject weakened actual leaf SIGKILL/SQL-health proof and logs, read-only."""
import argparse,copy,importlib.util,json,sys
from pathlib import Path
sys.dont_write_bytecode=True

def main():
 p=argparse.ArgumentParser(description=__doc__);p.add_argument('--root',type=Path,required=True);p.add_argument('--output',type=Path,required=True);a=p.parse_args()
 repo=Path('/home/exedev/js-wf') if Path(__file__).resolve().parent==Path('/tmp') else Path(__file__).resolve().parents[1];sys.path.insert(0,str(repo/'scripts'))
 import sql_leaf_transport_fault
 spec=importlib.util.spec_from_file_location('review',repo/'scripts/review-postgres-domain-projection.py');r=importlib.util.module_from_spec(spec);spec.loader.exec_module(r)
 paths=list((a.root/'originals').rglob('projection-fault-proof.json'));assert len(paths)==1;case=paths[0].parent;proof=json.loads(paths[0].read_text());rev=json.loads((a.root/'execution.json').read_text())['source'];log=(a.root/'native.log').read_text()
 positive=sql_leaf_transport_fault.validate(case,proof,rev,log,r.verify_child);rejected=[]
 def check(name,alter=None,alter_log=None):
  value=copy.deepcopy(proof);actual_log=log
  if alter:alter(value['leaf_transport_fault'])
  if alter_log:actual_log=alter_log(log)
  try:sql_leaf_transport_fault.validate(case,value,rev,actual_log,r.verify_child)
  except (AssertionError,ValueError,KeyError,IndexError):rejected.append(name)
  else:raise ValueError('weakened actual fault accepted: '+name)
 for name,value in [('baseline_rows',0),('baseline_rows',50000),('baseline_rows',True),('admitted_rows',50000),('admitted_rows',0),('sql_sessions_after_reap',1),('sql_sessions_after_reap',False),('sql_healthy_after_reap',False),('sql_healthy_after_reap',1),('sql_backend_terminated',True),('sql_backend_terminated',0)]:
  check(name+':'+repr(value),lambda f,n=name,v=value:f.__setitem__(n,v))
 for name,value in [('phase','replacement'),('exit_code',0),('wait_error',''),('exit_signal','killed'),('sha256','0'*64),('stat','0 (fake) R '+ '0 '*25),('environment',{}),('build_info','vcs.modified=true'),('leaf_transport',False),('nats_target','nats://127.0.0.1:1'),('wire_root','/tmp/unrelated'),('writer_backend_pid',0)]:
  check('child.'+name,lambda f,n=name,v=value:f['child'].__setitem__(n,v))
 check('child domain',lambda f:f['child']['argv'].__setitem__(4,'OTHER'))
 check('child duplicate',lambda f:f['child'].__setitem__('pid',proof['standalone_projectors'][1]['pid']))
 check('no partial progress',lambda f:f.__setitem__('admitted_rows',f['baseline_rows']))
 check('fault before child admission',lambda f:f.__setitem__('fault_started',proof['standalone_projectors'][1]['admitted_at']))
 check('SQL health before reap',lambda f:f.__setitem__('sql_checked_at',f['fault_started']))
 check('heal before fatal child',lambda f:f.__setitem__('healed_at',f['fault_started']))
 for marker in ['SQL leaf SIGKILL:','SQL leaf replacement:','SQL leaf transport fault:','SQL projector leaf wire: phase=leaf-fault ']:
  line=next(s for s in log.splitlines() if marker in s)
  check('missing '+marker,alter_log=lambda s,l=line:s.replace(l,''))
  check('duplicate '+marker,alter_log=lambda s,l=line:s+'\n'+l+'\n')
 a.output.parent.mkdir(parents=True,exist_ok=True);a.output.write_text(json.dumps({'source':rev,'actual_proof_accepted':True,'rejected_mutations':len(rejected),'mutations':rejected,'actual_fault_child_pid':positive['fault']['child']['pid'],'wire':positive['wire'],'scope':'Actual admitted leaf-fault proof/native-log mutations only; full original recovery/stock-process identity and SQL startup controls are separate.'},indent=2)+'\n');print(json.dumps({'actual_proof_accepted':True,'rejected_mutations':len(rejected)}))
if __name__=='__main__':main()
