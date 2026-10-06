import copy
import importlib.util
from pathlib import Path
import unittest
spec=importlib.util.spec_from_file_location('combined_review',Path(__file__).with_name('review-fanout-combined.py'))
r=importlib.util.module_from_spec(spec);spec.loader.exec_module(r)

class CombinedPhysicalDrain(unittest.TestCase):
    def fixture(self):
        at='2026-10-06T04:00:00Z';after='2026-10-06T04:00:01Z';deadline='2026-10-06T04:05:00Z'
        peers=[]
        for node in range(3):
            peers.append(dict(physical=dict(node=node,started=at,finished=after,state=dict(server_id='peer'+str(node),account_details=[dict(stream_detail=[dict(name='WF_RUN',state=dict(messages=0,consumer_count=64))])])),
                              queue=dict(ts=after,state=dict(messages=0,consumer_count=64),config=dict(num_replicas=3),cluster=dict(leader='leader',replicas=[dict(current=True),dict(current=True)])),
                              consumers=[dict(name=f'WF_P_{part:02d}',num_pending=0,num_ack_pending=0,ts=after) for part in range(64)]))
        return dict(workers_joined=True,elapsed_ns=2_000_000_000,case_deadline=deadline,all_three_peers=peers),at
    def test_original_drain_and_local_physical_witness(self):
        drain,heal=self.fixture();self.assertTrue(r.review_drain(drain,heal))
    def test_incomplete_or_outside_original_case(self):
        for mutation in ('unjoined','bool_elapsed','too_slow','missing_peer','legacy_api_only','local_retained','pre_heal','post_deadline','api_retained','replica_offline','durable_missing','durable_duplicate','pending','ack_pending','durable_late','api_late'):
            with self.subTest(mutation=mutation):
                drain,heal=self.fixture();peer=drain['all_three_peers'][0]
                if mutation=='unjoined':drain['workers_joined']=False
                elif mutation=='bool_elapsed':drain['elapsed_ns']=True
                elif mutation=='too_slow':drain['elapsed_ns']=300_000_000_000
                elif mutation=='missing_peer':drain['all_three_peers'].pop()
                elif mutation=='legacy_api_only':del peer['physical']
                elif mutation=='local_retained':peer['physical']['state']['account_details'][0]['stream_detail'][0]['state']['messages']=1
                elif mutation=='pre_heal':peer['physical']['started']='2026-10-06T03:59:59Z'
                elif mutation=='post_deadline':peer['physical']['finished']='2026-10-06T04:05:00.000000001Z'
                elif mutation=='api_retained':peer['queue']['state']['messages']=1
                elif mutation=='replica_offline':peer['queue']['cluster']['replicas'][0]['offline']=True
                elif mutation=='durable_missing':peer['consumers'].pop()
                elif mutation=='durable_duplicate':peer['consumers'][1]['name']=peer['consumers'][0]['name']
                elif mutation=='pending':peer['consumers'][0]['num_pending']=1
                elif mutation=='ack_pending':peer['consumers'][0]['num_ack_pending']=1
                elif mutation=='durable_late':peer['consumers'][0]['ts']='2026-10-06T04:05:00.000000001Z'
                elif mutation=='api_late':peer['queue']['ts']='2026-10-06T04:05:00.000000001Z'
                with self.assertRaises(ValueError):r.review_drain(drain,heal)

class CombinedStartupReview(unittest.TestCase):
    def fixture(self):
        deadline='2026-10-06T04:05:00Z'
        return dict(worker='parent-after-children',error='<nil>',original_case_deadline=deadline,
                    attempts=[dict(started='2026-10-06T04:01:00Z',finished='2026-10-06T04:01:05Z',error='signal stream: context deadline exceeded',retryable=True),
                              dict(started='2026-10-06T04:01:05.1Z',finished='2026-10-06T04:01:06Z',retryable=False)]),deadline
    def test_bounded_transient_startup_and_success(self):
        record,deadline=self.fixture();self.assertEqual(r.review_startup(record,'parent-after-children',deadline),2)
    def test_invalid_startup_records(self):
        for mutation in ('identity','error','deadline','missing','permanent','retryable_success','failed_final','reordered','late'):
            with self.subTest(mutation=mutation):
                record,deadline=self.fixture()
                if mutation=='identity':record['worker']='wrong'
                elif mutation=='error':record['error']='deadline exceeded'
                elif mutation=='deadline':record['original_case_deadline']='2026-10-06T04:06:00Z'
                elif mutation=='missing':record['attempts']=[]
                elif mutation=='permanent':record['attempts'][0]['retryable']=False
                elif mutation=='retryable_success':record['attempts'][1]['retryable']=True
                elif mutation=='failed_final':record['attempts'][1]['error']='unavailable'
                elif mutation=='reordered':record['attempts'][1]['started']='2026-10-06T04:01:00Z'
                elif mutation=='late':record['attempts'][1]['finished']='2026-10-06T04:05:00.000000001Z'
                with self.assertRaises(ValueError):r.review_startup(record,'parent-after-children',deadline)

if __name__=='__main__':unittest.main()
