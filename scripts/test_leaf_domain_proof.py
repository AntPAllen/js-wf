"""Regress destructive omissions and bypasses against the captured native proof."""
import copy
import json
from pathlib import Path
import unittest
from leaf_domain_proof import validate

class LeafDomainProofControls(unittest.TestCase):
    def test_native_positive_and_omission_bypass_controls(self):
        baseline=json.loads((Path(__file__).resolve().parents[1]/'docs/scale/leaf-domain-retirement-2026-10-07/native-race/leaf-domain-proof.json').read_text())
        validate(baseline)
        changes=[lambda p:p.update(scenario_passed=False),
                 lambda p:p.update(remote_domain='WFEDGE'),
                 lambda p:p.update(local_domain='WFRETIRE'),
                 lambda p:p.update(local_streams_after=1),
                 lambda p:p.update(leaf_disconnected=False),
                 lambda p:p.update(leaf_reconnected=False),
                 lambda p:p.update(runtime_server_ids=[p['before'][0]['id']]*3),
                 lambda p:p.update(after=p['before']),
                 lambda p:p.update(stopped=p['stopped'][:2]),
                 lambda p:p['leaf_after'].update(leafnodes=0),
                 lambda p:p['leaf_after']['leafs'][0].update(name='wrong'),
                 lambda p:p['leaf_after']['leafs'][0].update(account='OTHER'),
                 lambda p:p.update(cut_end=p['cut_start']),
                 lambda p:p.update(subjects={'$JS.API.STREAM.MSG.GET.OBJ_WF_BLOB':1}),
                 lambda p:p.update(subjects={})]
        for fragment in ('.DIRECT.GET.OBJ_WF_BLOB.', '.CONSUMER.CREATE.OBJ_WF_BLOB.', '.STREAM.MSG.GET.KV_WF_STATE'):
            changes.append(lambda p,f=fragment:p.update(subjects={k:v for k,v in p['subjects'].items() if f not in k}))
        for i,change in enumerate(changes):
            bad=copy.deepcopy(baseline);change(bad)
            with self.subTest(mutation=i),self.assertRaises((AssertionError,ValueError,KeyError)):
                validate(bad)

    def test_actual_sigkill_requires_exit_identity_and_all_client_disconnects(self):
        baseline=json.loads((Path(__file__).resolve().parents[1]/'docs/scale/leaf-domain-sigkill-2026-10-07/native-race/leaf-domain-proof.json').read_text())
        validate(baseline,'leaf-sigkill-hub-restart')
        variants=[lambda p:p.update(fault_profile='hub-restart'),lambda p:p.update(leaf_signal='terminated'),
                  lambda p:p.update(leaf_exit_observed=False),lambda p:p.update(leaf_pid_after=p['leaf_pid_before']),
                  lambda p:p.update(leaf_original_id=p['leaf_id']),lambda p:p.update(client_disconnects=[True,True]),
                  lambda p:p['leaf_before'].update(server_id=p['leaf_id'])]
        for i,change in enumerate(variants):
            bad=copy.deepcopy(baseline);change(bad)
            with self.subTest(mutation=i),self.assertRaises((AssertionError,ValueError,KeyError)):
                validate(bad,'leaf-sigkill-hub-restart')
        hub_only=json.loads((Path(__file__).resolve().parents[1]/'docs/scale/leaf-domain-retirement-2026-10-07/native-race/leaf-domain-proof.json').read_text())
        with self.assertRaises(AssertionError):validate(hub_only,'leaf-sigkill-hub-restart')

    def test_actual_expiry_requires_production_ttl_outage_and_successor(self):
        baseline=json.loads((Path(__file__).resolve().parents[1]/'docs/scale/leaf-domain-expiry-2026-10-07/native-race/leaf-domain-proof.json').read_text())
        validate(baseline,'leaf-sigkill-lease-expiry-hub-restart')
        variants=[lambda p:p.update(lease_ttl_seconds=30),lambda p:p.update(prior_epoch=0),
                  lambda p:p.update(terminal_epoch=p['prior_epoch']),lambda p:p.update(lease_revision=0),
                  lambda p:p.update(outage_end=p['outage_start']),lambda p:p.update(outage_start=p['cut_end']),
                  lambda p:p.update(fault_profile='leaf-sigkill-hub-restart'),lambda p:p.update(journal_records=0)]
        for i,change in enumerate(variants):
            bad=copy.deepcopy(baseline);change(bad)
            with self.subTest(mutation=i),self.assertRaises((AssertionError,ValueError,KeyError)):
                validate(bad,'leaf-sigkill-lease-expiry-hub-restart')
        shorter=json.loads((Path(__file__).resolve().parents[1]/'docs/scale/leaf-domain-sigkill-2026-10-07/native-race/leaf-domain-proof.json').read_text())
        with self.assertRaises(AssertionError):validate(shorter,'leaf-sigkill-lease-expiry-hub-restart')

if __name__=='__main__':unittest.main()
