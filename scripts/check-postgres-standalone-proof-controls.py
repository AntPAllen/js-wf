#!/usr/bin/env python3
"""Reject weakened packaged PostgreSQL proof records using actual retained evidence."""
import argparse
import copy
import importlib.util
import json
from pathlib import Path
import sys
sys.dont_write_bytecode=True


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root',type=Path,required=True)
    parser.add_argument('--output',type=Path,required=True)
    parser.add_argument('--projector-profile',choices=('standalone','standalone-leaf','standalone-leaf-startup','standalone-leaf-sigkill'),default='standalone')
    a=parser.parse_args();root=a.root.absolute()
    repo=Path(__file__).resolve().parents[1]
    if not (repo/'scripts/review-postgres-domain-projection.py').is_file():
        repo=Path('/home/exedev/js-wf')
    sys.path.insert(0,str(repo/'scripts'))
    spec=importlib.util.spec_from_file_location('projection_review',repo/'scripts/review-postgres-domain-projection.py')
    review=importlib.util.module_from_spec(spec);spec.loader.exec_module(review)
    fixtures=list((root/'originals').rglob('projection-fault-proof.json'));assert len(fixtures)==1
    proof=json.loads(fixtures[0].read_text());case=fixtures[0].parent
    revision=json.loads((root/'execution.json').read_text())['source']
    sdk=json.loads((root/'binary.json').read_text())['sha256'];log=(root/'native.log').read_text()
    review.verify_fault(proof,log,'',sdk,a.projector_profile);review.verify_standalone(proof,revision,case)
    rejected=[]
    def check(label,alter):
        value=copy.deepcopy(proof);alter(value)
        try:
            review.verify_fault(value,log,'',sdk,a.projector_profile)
            review.verify_standalone(value,revision,case)
        except (ValueError,KeyError,IndexError):rejected.append(label)
        else:raise ValueError('weakened proof accepted: '+label)
    for name,value in [('count',25000),('native_test_failed',True),('projector_profile','sdk'),('projection_domain','OTHER'),('projection_process_reaped_sigkill',False),('stopped_projection_lag',99999),('writer_backend_termination_confirmed',False),('final_lag',1),('standalone_replacement_clean_sigterm',False)]:
        check(name,lambda p,n=name,v=value:p.__setitem__(n,v))
    for name,value in [('source','0'*40),('package','js-wf/integration'),('sha256','0'*64),('build_info','vcs.modified=true')]:
        check('binary.'+name,lambda p,n=name,v=value:p['standalone_binary'].__setitem__(n,v))
    check('missing phase',lambda p:p.__setitem__('standalone_projectors',p['standalone_projectors'][:2]))
    for index,name,value in [(0,'exit_signal','terminated'),(0,'exit_code',0),(0,'sha256','0'*64),(0,'stat','0 (fake) R '+ '0 '*25),(1,'writer_backend_pid',0),(1,'exit_code',0),(1,'application_name','unrelated'),(1,'phase','replacement'),(1,'environment',{}),(1,'build_info','path js-wf/integration'),(2,'exit_code',1),(2,'admitted_at',proof['fault_started'])]:
        check(f'child{index}.{name}',lambda p,i=index,n=name,v=value:p['standalone_projectors'][i].__setitem__(n,v))
    check('child domain',lambda p:p['standalone_projectors'][1]['argv'].__setitem__(4,'OTHER'))
    check('wrong command',lambda p:p['standalone_projectors'][1]['argv'].__setitem__(5,'lag'))
    check('duplicate pid',lambda p:p['standalone_projectors'][1].__setitem__('pid',p['standalone_projectors'][0]['pid']))
    for name,altered in [('missing workload', 'PASS\n'),('native skip',log.replace('--- PASS:','--- SKIP:',1)),('wrong prefix',log.replace('wrong_prefix_requests=0','wrong_prefix_requests=1'))]:
        try:review.verify_fault(proof,altered,'',sdk,a.projector_profile)
        except ValueError:rejected.append(name)
        else:raise ValueError('weakened native log accepted: '+name)
    a.output.parent.mkdir(parents=True,exist_ok=True)
    a.output.write_text(json.dumps(dict(source=revision,accepted_actual_native_proof=True,rejected_mutations=len(rejected),mutations=rejected,scope='Actual full50000 packaged daemon proof and raw-log coverage controls; read-only original fixture.'),indent=2)+'\n')
    print(json.dumps(dict(rejected_mutations=len(rejected),actual_proof_accepted=True)))


if __name__=='__main__':main()
