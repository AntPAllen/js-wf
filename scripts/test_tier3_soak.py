import hashlib,importlib.util,json,os,tarfile,tempfile,unittest
from pathlib import Path
from unittest.mock import patch
spec=importlib.util.spec_from_file_location('soak',Path(__file__).with_name('run-tier3-soak.py'))
soak=importlib.util.module_from_spec(spec);spec.loader.exec_module(soak)

class SoakProducerTests(unittest.TestCase):
 def test_memory_budget_is_explicit_and_does_not_inherit_or_change_fault_gates(self):
  with patch.dict(os.environ,{'GOMEMLIMIT':'off','GOMAXPROCS':'99'}):
   plain,args,flags=soak.execution('journal','10m',1,Path('/tmp/f'),'sigkill',False)
   larger,larger_args,larger_flags=soak.execution('journal','10m',1,Path('/tmp/f'),'sigkill',False,memory_limit='2GiB')
  self.assertEqual(plain['GOMEMLIMIT'],'512MiB')
  self.assertEqual(larger['GOMEMLIMIT'],'2GiB')
  self.assertEqual(larger['GOMAXPROCS'],'2')
  self.assertEqual(args,larger_args)
  self.assertEqual(flags,larger_flags)
  self.assertEqual(plain['WF_TIER3_SYNC_INTERVAL'],larger['WF_TIER3_SYNC_INTERVAL'])
  for value in ['off','0','16GiB','2GB',None]:
   with self.assertRaises(ValueError):soak.execution('journal','10m',1,Path('/tmp/f'),'sigkill',False,memory_limit=value)
 def test_long_clock_execution_requires_actual_admission_and_matching_deadline(self):
  with patch.dict(os.environ,{'WF_TIER3_UPGRADE_START_GAP':'1','WF_TIER3_MATRIX_MUTATION':'omit_terminal','WF_TIER3_WORKER_KILL_CHILD':'1'}):
   env,args,flags=soak.execution('server_clock_ahead','24h',7,Path('/tmp/isolated'),'sigkill',False)
  self.assertNotIn('WF_TIER3_MATRIX_MUTATION',env)
  self.assertNotIn('WF_TIER3_WORKER_KILL_CHILD',env)
  self.assertNotIn('WF_TIER3_UPGRADE_START_GAP',env)
  self.assertEqual(env['WF_TIER3_COMMON_CLOCK'],'1')
  self.assertEqual(env['WF_TIER3_CLOCK_TIMER_CUT'],'1')
  self.assertEqual(env['FAULT_SEED'],'7')
  self.assertIn('-test.timeout=24h20m',args)
  self.assertIn('--require-common-timer-clock',flags)
  self.assertIn('--require-clock-timer-cut',flags)
 def test_requested_gap_shutdown_and_clock_guard_cannot_be_silently_dropped(self):
  env,args,flags=soak.execution('rolling_upgrade','10m',1,Path('/tmp/f'),'ldm',True)
  self.assertEqual(env['WF_TIER3_UPGRADE_SHUTDOWN'],'ldm')
  self.assertEqual(env['WF_TIER3_UPGRADE_START_GAP'],'1')
  self.assertIn('--require-start-scan-progress',flags)
  self.assertIn('--require-upgrade-start-gap',flags)
  self.assertIn('ldm',flags)
  _,args,_=soak.execution('worker_clock','35s',1,Path('/tmp/f'),'sigkill',False)
  self.assertIn('TestTier3WorkerClockNormalizationPreservesRawEvidence',args[0])
  for row,duration,seed,shutdown,gap in [('journal','24h',1,'ldm',False),('journal','24h',1,'sigkill',True),('journal','24h',0,'sigkill',False),('journal','1h',1,'sigkill',False)]:
   with self.assertRaises(ValueError):soak.execution(row,duration,seed,Path('/tmp/f'),shutdown,gap)
 def test_batched_audit_and_trace_are_explicit_and_do_not_change_budget(self):
  with patch.dict(os.environ,{'WF_TIER3_BATCHED_RETAINED_AUDIT':'1','WF_TIER3_RETAINED_AUDIT_TRACE':'1'}):
   plain,plain_args,plain_flags=soak.execution('journal','24h',1,Path('/tmp/f'),'sigkill',False)
   bulk,bulk_args,bulk_flags=soak.execution('journal','24h',1,Path('/tmp/f'),'sigkill',False,True,True)
  self.assertNotIn('WF_TIER3_BATCHED_RETAINED_AUDIT',plain)
  self.assertNotIn('WF_TIER3_RETAINED_AUDIT_TRACE',plain)
  self.assertEqual(bulk['WF_TIER3_BATCHED_RETAINED_AUDIT'],'1')
  self.assertEqual(bulk['WF_TIER3_RETAINED_AUDIT_TRACE'],'1')
  self.assertEqual(plain_args,bulk_args)
  self.assertEqual(plain_flags,bulk_flags)
 def test_streaming_state_mode_is_explicit_and_preserves_gate_arguments(self):
  with patch.dict(os.environ,{'WF_TIER3_STREAMING_STATE_RETAINED_AUDIT':'1'}):
   plain,args,flags=soak.execution('journal','10m',1,Path('/tmp/f'),'sigkill',False)
   streaming,new_args,new_flags=soak.execution('journal','10m',1,Path('/tmp/f'),'sigkill',False,streaming_state_retained_audit=True)
  self.assertNotIn('WF_TIER3_STREAMING_STATE_RETAINED_AUDIT',plain)
  self.assertEqual(streaming['WF_TIER3_STREAMING_STATE_RETAINED_AUDIT'],'1')
  self.assertNotIn('WF_TIER3_BATCHED_RETAINED_AUDIT',streaming)
  self.assertEqual(args,new_args)
  self.assertEqual(flags,new_flags)
  self.assertEqual(plain['GOMEMLIMIT'],streaming['GOMEMLIMIT'])
  self.assertEqual(plain['WF_TIER3_SYNC_INTERVAL'],streaming['WF_TIER3_SYNC_INTERVAL'])
  with self.assertRaises(ValueError):
   soak.execution('journal','10m',1,Path('/tmp/f'),'sigkill',False,batched_retained_audit=True,streaming_state_retained_audit=True)
 def test_archive_keeps_original_store_bytes_and_compiled_source_with_full_readback(self):
  with tempfile.TemporaryDirectory() as d:
   root=Path(d);(root/'fixture').mkdir();(root/'source').mkdir()
   (root/'fixture/store.blk').write_bytes(bytes(range(256))*4096)
   (root/'source/runtime.go').write_text('package runtime\n')
   (root/'source/.git').write_text('external checkout pointer')
   soak.archive_originals(root)
   manifest=json.loads((root/'archive-manifest.json').read_text())
   with tarfile.open(root/'originals.tar.gz') as tar:
    actual={m.name:hashlib.sha256(tar.extractfile(m).read()).hexdigest() for m in tar.getmembers()}
   self.assertEqual(actual,manifest['files'])
   self.assertEqual(set(actual),{'fixture/store.blk','source/runtime.go'})
   self.assertEqual(soak.sha(root/'originals.tar.gz'),manifest['archive_sha256'])
   self.assertTrue((root/'fixture/store.blk').exists())
   self.assertFalse((root/'originals.tmp.tar.gz').exists())
