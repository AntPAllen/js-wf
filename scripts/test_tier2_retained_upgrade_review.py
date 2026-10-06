import copy
from datetime import datetime,timedelta,timezone
import importlib.util
from pathlib import Path
import unittest
spec=importlib.util.spec_from_file_location('upgrade_review',Path(__file__).with_name('review-tier2-retained-upgrade.py'))
r=importlib.util.module_from_spec(spec);spec.loader.exec_module(r)
class UpgradeReview(unittest.TestCase):
    def fixture(self):
        base=datetime(2026,10,6,tzinfo=timezone.utc);stamp=lambda seconds:(base+timedelta(seconds=seconds)).isoformat()
        faults=[];snapshots={};versions=['2.11.17']*3
        def snapshot(phase,at,messages):
            return dict(phase=phase,peers=[dict(node=n,started=stamp(at),finished=stamp(at+0.1),state=dict(server_id='peer'+str(n),account_details=[dict(stream_detail=[dict(name='WF_RUN',state=dict(messages=messages,consumer_count=64))])])) for n in range(3)])
        for index,node in enumerate((2,1,0)):
            at=30+index*270;after=versions.copy();after[node]='2.15.0'
            fault=dict(node=node,scheduled=stamp(at),killed=stamp(at+0.5),healed=stamp(at+8),versions_before=versions.copy(),versions_after=after.copy())
            faults.append(fault);versions=after
            key=f'upgrade-{r.ns(fault["scheduled"])}-{node}'
            snapshots[key+'-before']=snapshot(key+'-before',at,3)
            snapshots[key+'-healed']=snapshot(key+'-healed',at+8.1,2)
        snapshots['drained']=snapshot('drained',610,0)
        return faults,snapshots
    def test_original_upgrades_and_seven_physical_snapshots(self):
        faults,snapshots=self.fixture();self.assertEqual(len(r.review_upgrade_faults(faults,snapshots.__getitem__)),7)
    def test_fault_corruption(self):
        for mutation in ('missing','duplicate_node','bool_node','error','cadence','reversed','wrong_before','wrong_target','other_peer','late_before','early_healed','early_drain'):
            with self.subTest(mutation=mutation):
                faults,snapshots=self.fixture();key=next(iter(snapshots));healed=key.replace('-before','-healed')
                if mutation=='missing':faults.pop()
                elif mutation=='duplicate_node':faults[1]['node']=2
                elif mutation=='bool_node':faults[0]['node']=False
                elif mutation=='error':faults[0]['error']='restart failed'
                elif mutation=='cadence':faults[1]['scheduled']=faults[0]['scheduled']
                elif mutation=='reversed':faults[0]['healed']=faults[0]['scheduled']
                elif mutation=='wrong_before':faults[1]['versions_before']=['2.11.17']*3
                elif mutation=='wrong_target':faults[0]['versions_after'][2]='2.12.0'
                elif mutation=='other_peer':faults[0]['versions_after'][0]='2.15.0'
                elif mutation=='late_before':snapshots[key]['peers'][0]['finished']=faults[0]['healed']
                elif mutation=='early_healed':snapshots[healed]['peers'][0]['started']=faults[0]['killed']
                elif mutation=='early_drain':snapshots['drained']['peers'][0]['started']=faults[2]['scheduled']
                with self.assertRaises(ValueError):r.review_upgrade_faults(faults,snapshots.__getitem__)
    def test_physical_snapshot_corruption(self):
        for mutation in ('phase','missing','duplicate_node','bool_node','duplicate_id','empty_id','error','reversed','nano_reversed','stream_missing','stream_duplicate','messages_bool','messages_negative','durables','retained'):
            with self.subTest(mutation=mutation):
                _,snapshots=self.fixture();record=snapshots['drained'];peer=record['peers'][0];streams=peer['state']['account_details'][0]['stream_detail']
                if mutation=='phase':record['phase']='before'
                elif mutation=='missing':record['peers'].pop()
                elif mutation=='duplicate_node':record['peers'][1]['node']=0
                elif mutation=='bool_node':peer['node']=False
                elif mutation=='duplicate_id':record['peers'][1]['state']['server_id']=peer['state']['server_id']
                elif mutation=='empty_id':peer['state']['server_id']=''
                elif mutation=='error':peer['error']='HTTP unavailable'
                elif mutation=='reversed':peer['finished']='2026-10-05T23:59:59Z'
                elif mutation=='nano_reversed':peer.update(started='2026-10-06T00:00:00.000000002Z',finished='2026-10-06T00:00:00.000000001Z')
                elif mutation=='stream_missing':streams.clear()
                elif mutation=='stream_duplicate':streams.append(copy.deepcopy(streams[0]))
                elif mutation=='messages_bool':streams[0]['state']['messages']=False
                elif mutation=='messages_negative':streams[0]['state']['messages']=-1
                elif mutation=='durables':streams[0]['state']['consumer_count']=63
                elif mutation=='retained':streams[0]['state']['messages']=1
                with self.assertRaises(ValueError):r.review_physical_snapshot(record,'drained',True)
if __name__=='__main__':unittest.main()
