import copy
from datetime import datetime,timedelta,timezone
import importlib.util
from pathlib import Path
import unittest
spec=importlib.util.spec_from_file_location('clock_review',Path(__file__).with_name('review-tier2-retained-server-clock.py'))
r=importlib.util.module_from_spec(spec);spec.loader.exec_module(r)
class ServerClockReview(unittest.TestCase):
    def fixture(self,offset):
        base=datetime(2026,10,6,tzinfo=timezone.utc)
        stamp=lambda second:(base+timedelta(seconds=second)).isoformat()
        return [dict(node=2,scheduled=stamp(i*30),healed=stamp(i*30+1),server_clocks=[dict(node=n,server_at=stamp(i*30+(offset//1_000_000_000 if n==2 else 0)),offset_ns=offset if n==2 else 0) for n in range(3)]) for i in range(19)]
    def test_accepts_both_original_clock_patterns(self):
        for offset in (-60_000_000_000,60_000_000_000):r.review_clock_faults(self.fixture(offset),offset)
    def test_rejects_corrupted_observation_patterns(self):
        for offset in (-60_000_000_000,60_000_000_000):
            for mutation in ('count','error','cadence','reversed','node','missing_peer','duplicate_peer','bool_node','wrong_sign','skewed_neutral','bool_offset','invalid_timestamp'):
                with self.subTest(offset=offset,mutation=mutation):
                    f=self.fixture(offset)
                    if mutation=='count':f.pop()
                    elif mutation=='error':f[0]['error']='clock failed'
                    elif mutation=='cadence':f[1]['scheduled']=f[0]['scheduled']
                    elif mutation=='reversed':f[0]['healed']='2026-10-05T23:59:59.999999999Z'
                    elif mutation=='node':f[0]['node']=1
                    elif mutation=='missing_peer':f[0]['server_clocks'].pop()
                    elif mutation=='duplicate_peer':f[0]['server_clocks'][1]['node']=0
                    elif mutation=='bool_node':f[0]['server_clocks'][0]['node']=False
                    elif mutation=='wrong_sign':f[0]['server_clocks'][2]['offset_ns']=-offset
                    elif mutation=='skewed_neutral':f[0]['server_clocks'][0]['offset_ns']=2_000_000_001
                    elif mutation=='bool_offset':f[0]['server_clocks'][0]['offset_ns']=False
                    elif mutation=='invalid_timestamp':f[0]['server_clocks'][0]['server_at']='missing'
                    with self.assertRaises(ValueError):r.review_clock_faults(f,offset)
    def test_latency_artifact_is_a_complete_list_with_exact_clock_correction(self):
        r.review_latency_offsets([dict(event='terminal',server_clock_offset_ns=60_000_000_000)],60_000_000_000,1)
        for samples,count in (([],1),([dict(event='terminal',server_clock_offset_ns=-60_000_000_000)],1),([dict(event='terminal',server_clock_offset_ns=60_000_000_000)],2),([dict(event='terminal')],1)):
            with self.assertRaises(ValueError):r.review_latency_offsets(samples,60_000_000_000,count)
if __name__=='__main__':unittest.main()
