#!/usr/bin/env python3
"""Reject weakened actual SQL startup cancellation proof and logs, read-only."""
import argparse,copy,importlib.util,json,sys
from pathlib import Path
sys.dont_write_bytecode=True

def main():
 parser=argparse.ArgumentParser(description=__doc__);parser.add_argument('--root',type=Path,required=True);parser.add_argument('--output',type=Path,required=True);parser.add_argument('--expected-signal',choices=('SIGTERM','SIGINT'),default='SIGTERM');args=parser.parse_args()
 repo=Path('/home/exedev/js-wf') if Path(__file__).resolve().parent==Path('/tmp') else Path(__file__).resolve().parents[1]
 sys.path.insert(0,str(repo/'scripts'))
 import sql_startup_cancellation
 spec=importlib.util.spec_from_file_location('review',repo/'scripts/review-postgres-domain-projection.py');review=importlib.util.module_from_spec(spec);spec.loader.exec_module(review)
 paths=list((args.root/'originals').rglob('projection-fault-proof.json'));assert len(paths)==1
 case=paths[0].parent;proof=json.loads(paths[0].read_text());revision=json.loads((args.root/'execution.json').read_text())['source'];log=(args.root/'native.log').read_text()
 signal_field='sigint_sent_at' if args.expected_signal=='SIGINT' else 'sigterm_sent_at'
 accepted=sql_startup_cancellation.validate(case,proof,revision,log,review.verify_child,expected_signal=args.expected_signal)
 rejected=[]
 def check(name,change=None,alter_log=None):
  value=copy.deepcopy(proof);actual_log=log
  if change:change(value['sql_startup_cancellation'])
  if alter_log:actual_log=alter_log(actual_log)
  try:sql_startup_cancellation.validate(case,value,revision,actual_log,review.verify_child,expected_signal=args.expected_signal)
  except (AssertionError,ValueError,KeyError,IndexError):rejected.append(name)
  else:raise ValueError('weakened SQL startup boundary accepted: '+name)
 for name,value in [('blocked_backend_pid',0),('blocker_backend_pid',0),('blocking_pid_confirmed',False),('blocking_pid_confirmed',1),('wait_event_type','Client'),('wait_event','advisory'),('query','SELECT 1'),('residual_sessions',1),('residual_locks',1),('rows',1),('residual_sessions',False),('projection_durables_absent',False),('projection_durables_absent',1)]:
  check(name+':'+repr(value),lambda b,n=name,v=value:b.__setitem__(n,v))
 for name,value in [('phase','initial'),('exit_code',1),('exit_signal','terminated'),('wait_error','context canceled'),('startup_blocker_backend_pid',0),('writer_backend_pid',0),('leaf_transport',False),('nats_target','nats://127.0.0.1:1'),('sha256','0'*64),('environment',{}),('build_info','vcs.modified=true'),('stat','0 (fake) R '+ '0 '*25)]:
  check('child.'+name,lambda b,n=name,v=value:b['child'].__setitem__(n,v))
 check('child domain',lambda b:b['child']['argv'].__setitem__(4,'OTHER'))
 check('child reused pid',lambda b:b['child'].__setitem__('pid',proof['standalone_projectors'][0]['pid']))
 check('backend is blocker',lambda b:b.__setitem__('blocked_backend_pid',b['blocker_backend_pid']))
 check('release before signal',lambda b:b.__setitem__('blocker_released_at',b['lock_held_at']))
 check('residual before reap',lambda b:b.__setitem__('residual_checked_at',b[signal_field]))
 check('signal before admission',lambda b:b.__setitem__(signal_field,b['lock_held_at']))
 check('different signal',lambda b:b.__setitem__('signal','SIGTERM' if args.expected_signal=='SIGINT' else 'SIGINT'))
 check('missing signal timestamp',lambda b:b.pop(signal_field))
 check('contradictory signal timestamp',lambda b:b.__setitem__('sigterm_sent_at' if args.expected_signal=='SIGINT' else 'sigint_sent_at',b[signal_field]))
 if args.expected_signal=='SIGINT':check('missing explicit SIGINT',lambda b:b.pop('signal'))
 boundaryline=next(l for l in log.splitlines() if 'SQL startup cancelled:' in l)
 wireline=next(l for l in log.splitlines() if 'SQL projector leaf wire: phase=sql-startup ' in l)
 for name,line in [('boundary',boundaryline),('wire',wireline)]:
  check('missing '+name,alter_log=lambda s,l=line:s.replace(l,''))
  check('duplicate '+name,alter_log=lambda s,l=line:s+'\n'+l+'\n')
 args.output.parent.mkdir(parents=True,exist_ok=True)
 args.output.write_text(json.dumps({'source':revision,'actual_proof_accepted':True,'rejected_mutations':len(rejected),'mutations':rejected,'actual_startup_child_pid':accepted['boundary']['child']['pid'],'scope':'Actual SQL lock startup proof and native logs only; inherited full50000 proof controls are separate.'},indent=2)+'\n')
 print(json.dumps({'actual_proof_accepted':True,'rejected_mutations':len(rejected)}))
if __name__=='__main__':main()
