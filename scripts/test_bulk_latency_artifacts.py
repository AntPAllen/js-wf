import copy,importlib.util,json,tempfile,unittest
from pathlib import Path
spec=importlib.util.spec_from_file_location('row',Path(__file__).with_name('check-tier3-journal-row.py'))
row=importlib.util.module_from_spec(spec);spec.loader.exec_module(row)

class BulkArtifactChecks(unittest.TestCase):
 def setUp(self):
  self.tmp=tempfile.TemporaryDirectory();self.addCleanup(self.tmp.cleanup);self.root=Path(self.tmp.name)
  self.report=dict(invocations=2,journal_entries=8,cells={'matrixshort':{'invocations':2}})
  counts=dict(Invocations=2,Journals=2,Entries=8,Terminal=2)
  cuts={name:dict(First=1 if count else 0,Last=count,Messages=count,Bytes=count*100,Consumers=0) for name,count in [('WF_INV',2),('WF_JRN',8),('KV_WF_STATE',2),('WF_SIG',0)]}
  self.proof=dict(error='<nil>',after_error='<nil>',final_integrity_verified=True,before_report=counts,after_report=copy.deepcopy(counts),stage_limit_ns=360_000_000_000,elapsed_ns=1_000_000_000,point_comparison_requested=True,every_sample_matches_point=True,bulk_stats=dict(source_cuts=cuts,records={k:v['Messages'] for k,v in cuts.items() if v['Messages']},invocation_cutoff=2,charged_bytes=1000))
  self.samples=[dict(type='matrixshort',id=str(i),event=event,enabled='2026-10-06T00:00:00Z',observed='2026-10-06T00:00:01Z',delay_ns=1_000_000_000) for i in range(2) for event in ['start','terminal']]
 def check(self):
  (self.root/'bulk-latency-audit.json').write_text(json.dumps(self.proof));(self.root/'latencies.json').write_text(json.dumps(self.samples))
  return row.check_bulk_latency_artifacts(self.root,self.report,True)
 def test_complete_receipt_and_absent_empty_stream_counter(self):
  result=self.check();self.assertEqual(result['invocations'],2);self.assertTrue(result['complete_point_equivalence'])
 def test_failed_or_incomplete_receipt_is_rejected(self):
  for key,value in [('error','timeout'),('after_error','timeout'),('final_integrity_verified',False),('every_sample_matches_point',False),('point_comparison_requested',False)]:
   with self.subTest(key=key):
    original=self.proof[key];self.proof[key]=value
    with self.assertRaises(ValueError):self.check()
    self.proof[key]=original
 def test_count_change_or_original_budget_change_is_rejected(self):
  self.proof['after_report']['Entries']=7
  with self.assertRaises(ValueError):self.check()
  self.proof['after_report']['Entries']=8;self.proof['elapsed_ns']=360_000_000_000
  with self.assertRaises(ValueError):self.check()
  self.proof['elapsed_ns']=1;self.proof['stage_limit_ns']=600_000_000_000
  with self.assertRaises(ValueError):self.check()
 def test_incomplete_or_unknown_source_census_is_rejected(self):
  for mutation in [lambda x:x['records'].pop('WF_JRN'),lambda x:x['records'].update(WF_SIG=1),lambda x:x['records'].update(unknown=0),lambda x:x['records'].update(WF_SIG=False),lambda x:x.update(invocation_cutoff=1),lambda x:x.update(charged_bytes=4*1024**3)]:
   original=copy.deepcopy(self.proof['bulk_stats']);mutation(self.proof['bulk_stats'])
   with self.assertRaises(ValueError):self.check()
   self.proof['bulk_stats']=original
 def test_partial_duplicate_or_bad_timestamp_samples_are_rejected(self):
  for mutation in [lambda x:x.pop(),lambda x:x.append(copy.deepcopy(x[0])),lambda x:x[0].update(delay_ns=2),lambda x:x[0].update(type='unknown'),lambda x:x[0].update(server_clock_offset_ns=1)]:
   original=copy.deepcopy(self.samples);mutation(self.samples)
   with self.assertRaises(ValueError):self.check()
   self.samples=original
