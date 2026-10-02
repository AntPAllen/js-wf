import copy
import json
from pathlib import Path
import tempfile
import unittest
from test_tier3_journal_row import row, fixture


class AutomaticMembershipChecks(unittest.TestCase):
    def test_scope_remains_one_row(self):
        events = fixture('35s')
        for event in events:
            if 'Test' in event: event['Test'] = row.TESTS['auto_journal']
        events[0]['Output'] = events[0]['Output'].replace('row=journal', 'row=auto_journal')
        self.assertFalse(row.check(events, '35s', 'auto_journal')['clears_full_tier3_release'])

    def test_complete_claim_and_final_ownership_required(self):
        stamp = lambda second: f'2026-10-02T00:00:{second:02d}Z'
        members = [f'tier3-mixed-{n}' for n in range(5)]
        owners = [member for member,count in zip(members,[13,13,13,13,12]) for _ in range(count)]
        for mode in ('valid','missing_member','unbalanced','missing_partition','no_coordinator','wrong_replication',
                     'offline_replica','missing_write','wrong_cas','duplicate_revision','unknown_write',
                     'wrong_final_owner','unbracketed','missing_worker','wrong_controller','wrong_ttl','no_renewal'):
            with self.subTest(mode=mode), tempfile.TemporaryDirectory() as directory:
                root=Path(directory)
                initial=dict(observed=stamp(1), members=members.copy(), assignments=[dict(partition=p,owner=owner,revision=p+1) for p,owner in enumerate(owners)],
                             coordinator=dict(worker=members[0],epoch=1),coordinator_revision=1,membership_stream=dict(
                                 config=dict(name='KV_WF_MEMBERS',num_replicas=5,max_age=12_000_000_000,storage='file'),cluster=dict(leader='n0',replicas=[dict(current=True)]*4)))
                final=copy.deepcopy(initial);final['observed']=stamp(40);final['coordinator_revision']=2
                writes=[dict(controller=members[0],partition=p,owner=owner,expected=0,revision=p+1,before=stamp(0),after=stamp(1),error='') for p,owner in enumerate(owners)]
                dispatch=[dict(Worker=member,Stage='lease_acquired',RunSequence=n+1,Type='workflow',ID='invocation') for n,member in enumerate(members)]
                if mode=='missing_member':initial['members'].pop()
                elif mode=='unbalanced':initial['assignments'][0]['owner']=members[1]
                elif mode=='missing_partition':initial['assignments'].pop()
                elif mode=='no_coordinator':initial['coordinator']['epoch']=0
                elif mode=='wrong_replication':initial['membership_stream']['config']['num_replicas']=3
                elif mode=='offline_replica':initial['membership_stream']['cluster']['replicas'][0]['current']=False
                elif mode=='missing_write':writes.pop()
                elif mode=='wrong_cas':writes[0]['expected']=1
                elif mode=='duplicate_revision':writes[1]['revision']=writes[0]['revision']
                elif mode=='unknown_write':writes[0]['error']='outcome unknown'
                elif mode=='wrong_final_owner':
                    final['assignments'][0]['owner'],final['assignments'][13]['owner']=final['assignments'][13]['owner'],final['assignments'][0]['owner']
                elif mode=='unbracketed':final['observed']=stamp(31)
                elif mode=='missing_worker':dispatch.pop()
                elif mode=='wrong_controller':writes[0]['controller']='absent'
                elif mode=='wrong_ttl':initial['membership_stream']['config']['max_age']=30_000_000_000
                elif mode=='no_renewal':final['coordinator_revision']=initial['coordinator_revision']
                for name,data in [('automatic-initial.json',initial),('automatic-final.json',final),('automatic-writes.json',writes),('dispatch.json',dispatch),
                                  ('faults.json',[dict(killed=stamp(30),healed=stamp(35))])]:
                    (root/name).write_text(json.dumps(data))
                if mode=='valid':
                    result=row.check_automatic_artifacts(root,dict(confirmed_faults=1))
                    self.assertEqual(result['claimed_partitions'],64)
                    self.assertFalse(result['admits_membership_churn'])
                else:
                    with self.assertRaises(ValueError):row.check_automatic_artifacts(root,dict(confirmed_faults=1))
