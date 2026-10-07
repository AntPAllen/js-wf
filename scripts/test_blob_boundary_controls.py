import copy
import importlib.util
import json
from pathlib import Path
import unittest

REPO=Path(__file__).resolve().parents[1]
spec=importlib.util.spec_from_file_location('blob_controls',REPO/'scripts/run-blob-boundary-controls.py')
controls=importlib.util.module_from_spec(spec)
spec.loader.exec_module(controls)
CAPTURE=REPO/'docs/scale/online-blob-boundary-2026-10-07/native-race'


class BlobBoundaryControlsTests(unittest.TestCase):
    def setUp(self):
        self.log=(CAPTURE/'native.log').read_text()
        self.proofs={mode:json.loads((CAPTURE/(mode+'-boundary-proof.json')).read_text()) for mode in controls.MODES}

    def test_original_native_capture_is_a_counterexample_not_online_safety(self):
        result=controls.verify_proofs(self.log,self.proofs)
        self.assertTrue(result['counterexample_confirmed'])
        self.assertTrue(result['quiescent_control_retained'])
        self.assertIn('not safe online GC',result['scope'])

    def test_actual_log_rejects_omission_duplication_and_changed_outcomes(self):
        malformed=[self.log.replace('deleted=1 dangling=true','deleted=0 dangling=false'),
                   self.log.replace('deleted=0 dangling=false','deleted=1 dangling=true'),
                   self.log.replace('acked_ref=true','acked_ref=false'),
                   self.log.replace('--- PASS: '+controls.TEST+'/quiescent','--- SKIP: '+controls.TEST+'/quiescent'),
                   self.log+'DATA RACE\n']
        for mode,proof in self.proofs.items():
            malformed.append(self.log.replace('fresh_nuid='+proof['fresh_nuid'],'fresh_nuid='+proof['old_nuid']))
            line=next(line for line in self.log.splitlines() if 'mode='+mode+' ' in line)
            malformed.extend((self.log.replace(line,''),self.log.replace(line,line+'\n'+line),
                              self.log.replace(line,line+' trailing-unparsed-field')))
        malformed.append(self.log.replace(self.proofs['refresh_after_census']['server_id'],self.proofs['quiescent']['server_id']))
        for altered in malformed:
            with self.subTest(log=altered):
                self.assertNotEqual(self.log,altered)
                with self.assertRaises(AssertionError):controls.verify_log(altered)

    def test_actual_proofs_require_full_counts_and_bind_to_native_log(self):
        for mode in controls.MODES:
            variants=[]
            for key,value in [('mode','other'),('old_nuid','other'),('fresh_nuid','other'),
                              ('server_id','other'),('invocation_sequence',0),('invocation_sequence',True),
                              ('acknowledged_reference','input-missing'),('dangling',not self.proofs[mode]['dangling'])]:
                altered=copy.deepcopy(self.proofs);altered[mode][key]=value;variants.append(altered)
            for key in ('objects','referenced','eligible','deleted'):
                altered=copy.deepcopy(self.proofs);altered[mode]['sweep'][key]+=1;variants.append(altered)
            altered=copy.deepcopy(self.proofs);altered[mode]['sweep']['deleted']=bool(altered[mode]['sweep']['deleted']);variants.append(altered)
            altered=copy.deepcopy(self.proofs);altered.pop(mode);variants.append(altered)
            for altered in variants:
                with self.subTest(mode=mode,proofs=altered):
                    with self.assertRaises(AssertionError):controls.verify_proofs(self.log,altered)


if __name__=='__main__':unittest.main()
