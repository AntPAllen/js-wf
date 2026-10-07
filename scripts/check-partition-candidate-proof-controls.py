#!/usr/bin/env python3
"""Reject substitutions in an actual accepted closed candidate proof; never start a broker."""
import argparse
import copy
import importlib.util
import json
from pathlib import Path
import shutil
import tempfile

spec=importlib.util.spec_from_file_location('candidate_review',Path(__file__).with_name('review-local-tier2-partition.py'))
review=importlib.util.module_from_spec(spec);spec.loader.exec_module(review)


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--seed-root',type=Path,required=True)
    parser.add_argument('--source',required=True)
    parser.add_argument('--proof',type=Path,required=True)
    parser.add_argument('--output',type=Path,required=True)
    args=parser.parse_args()
    assert not args.output.exists()
    expected=review.candidate_binding(args.seed_root,args.source,args.proof)
    rejected=[]
    with tempfile.TemporaryDirectory(prefix='js-wf-candidate-proof-controls-') as directory:
        root=Path(directory)
        shutil.copytree(args.seed_root/'inputs',root/'inputs')
        for name in ['partition-server.bin','partition-server-input.json','partition-server-verification.json']:
            shutil.copyfile(args.seed_root/name,root/name)
        assert review.candidate_binding(root,args.source,args.proof)==expected
        originals={name:(root/name).read_bytes() for name in ['partition-server-input.json','partition-server-verification.json']}
        mutations=[('partition-server-input.json',key,value) for key,value in
                   [('parent_proof','docs/other'),('parent_proof_revision','0'*40),('captured','other.bin'),
                    ('parent_source','0'*40),('sha256','0'*64),('retained_parent_records',{})]]
        mutations += [('partition-server-verification.json',key,value) for key,value in
                      [('passed',False),('passed',1),('observed_peers',2),('observed_peers',3.0),
                       ('observed_peers','3'),('expected_sha256','0'*64),('server_profile','default')]]
        for name,key,value in mutations:
            changed=json.loads(originals[name]);changed[key]=value
            (root/name).write_text(json.dumps(changed))
            try:review.candidate_binding(root,args.source,args.proof)
            except (ValueError,KeyError,TypeError):rejected.append(dict(file=name,field=key,value=value))
            else:raise AssertionError('accepted candidate proof substitution: '+key)
            (root/name).write_bytes(originals[name])
        retained=root/'inputs/partition-server-proof'
        for path in sorted(retained.iterdir()):
            data=path.read_bytes();path.write_bytes(data+b'\n')
            try:review.candidate_binding(root,args.source,args.proof)
            except ValueError:rejected.append(dict(file=path.name,change='committed parent bytes'))
            else:raise AssertionError('accepted changed parent: '+path.name)
            path.write_bytes(data)
        binary=root/'partition-server.bin'
        with binary.open('r+b') as file:
            first=file.read(1);file.seek(0);file.write(bytes([first[0]^1]))
        try:review.candidate_binding(root,args.source,args.proof)
        except ValueError:rejected.append(dict(file=binary.name,change='one executable byte'))
        else:raise AssertionError('accepted changed executable')
    assert review.candidate_binding(args.seed_root,args.source,args.proof)==expected
    args.output.write_text(json.dumps(dict(actual_positive_accepted=True,actual_original_unchanged=True,
        source=args.source,candidate_sha256=expected,rejected_substitutions=rejected,count=len(rejected),
        scope='Candidate provenance substitutions on a fresh copied actual accepted fixture only; no native SDK/server rerun or broker opened.'),indent=2)+'\n')
    print('CANDIDATE_PROOF_SUBSTITUTIONS_REJECTED',len(rejected))


if __name__=='__main__':main()
