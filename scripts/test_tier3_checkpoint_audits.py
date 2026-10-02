import copy
import importlib.util
import json
from pathlib import Path
import tempfile
import unittest

spec=importlib.util.spec_from_file_location('tier3',Path(__file__).with_name('check-tier3-journal-row.py'))
module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module)

class CheckpointAuditTests(unittest.TestCase):
 def verify(self,rows,invocations=560):
  with tempfile.TemporaryDirectory() as folder:
   root=Path(folder);(root/'checkpoint-audits.json').write_text(json.dumps(rows))
   return module.check_checkpoint_audits(root,{'invocations':invocations})
 def rows(self):
  return [dict(batch=b,invocation_cutoff=b*28,started='2026-10-02T05:00:00Z',completed='2026-10-02T05:00:01Z',report=dict(Invocations=b*28,Journals=b*28,Terminal=b*28,Entries=b*28*4)) for b in (10,20)]
 def test_complete_and_smoke_without_checkpoint(self):
  self.assertEqual(self.verify(self.rows())['completed_cohort_audits'],2)
  self.assertEqual(self.verify(None,168)['completed_cohort_audits'],0)
 def test_missing_duplicate_and_reordered(self):
  rows=self.rows()
  for broken in ([],rows[:1],rows+rows[:1],list(reversed(rows))):
   with self.assertRaises(ValueError):self.verify(broken)
 def test_failed_unfinished_wrong_cohort_or_cutoff(self):
  for key,value in (('error','context canceled'),('completed','0001-01-01T00:00:00Z'),('invocation_cutoff',279),('invocation_cutoff',False)):
   rows=self.rows();rows[0][key]=value
   with self.subTest(key=key),self.assertRaises(ValueError):self.verify(rows)
  for key in ('Invocations','Journals','Terminal','Entries'):
   rows=self.rows();rows[0]['report'][key]-=1
   with self.subTest(key=key),self.assertRaises(ValueError):self.verify(rows)

if __name__=='__main__':unittest.main()
