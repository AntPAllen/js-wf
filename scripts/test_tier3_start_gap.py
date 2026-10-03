import base64,copy,hashlib,importlib.util,json,tempfile,unittest
from pathlib import Path
from datetime import datetime,timedelta,timezone
spec=importlib.util.spec_from_file_location('gap_guard',Path(__file__).with_name('check-tier3-journal-row.py'))
row=importlib.util.module_from_spec(spec);spec.loader.exec_module(row)
class StartGapChecks(unittest.TestCase):
 def fixture(self,root):
  at=lambda x:(datetime(2026,10,3,tzinfo=timezone.utc)+timedelta(seconds=x)).isoformat().replace('+00:00','Z')
  enc=lambda b:base64.b64encode(b).decode()
  inv=dict(Subject='wf.inv.matrixshort.cohort',Sequence=12,Data=enc(b'null'),Header={'Wf-Input-Sha256':[hashlib.sha256(b'null').hexdigest()]})
  receipt=dict(pid=123,server_id='old-id',version='2.11.17',invoked=at(31),at=at(31.1),run_subject='wf.run.7',run_data='matrixshort.cohort',invocation=inv)
  proof=dict(receipt=receipt,kill_started=at(31.2),killed=at(31.3),wait_error='signal: killed',signal='SIGKILL',retained=inv,run_messages=0,journal_absent=True)
  gap=dict(type='matrixshort',id='cohort',root='fault-1-start-gap',proof=proof,after_upgrade=copy.deepcopy(inv),repair_released=at(36))
  fault=dict(scheduled=at(30),start_gap=gap)
  directory=root/gap['root'];directory.mkdir()
  def save(path,data):path.write_text(json.dumps(data))
  save(directory/'upgrade-gap.json',gap);save(directory/'killed-gap.json',proof)
  entry=dict(kind='Completed',payload=dict(inv_seq=12,result=enc(b'42')))
  save(directory/'terminal.json',dict(Subject='wf.jrn.matrixshort.cohort',Data=enc(json.dumps(entry).encode())))
  save(directory/'completion.json',dict(verified=at(38),inv_seq=12,kill_to_terminal_ns=6700000000))
  save(root/'repairs.json',[dict(kind='start',type='matrixshort',id='cohort',outcome='acknowledged',at=at(37))])
  calls=[dict(op='start',args=dict(type='matrixshort',id='cohort'),result=dict(status=status,inv_seq=seq),invoke_ts=at(begin),return_ts=at(end)) for status,seq,begin,end in [('unknown',0,31,31.3),('already_started',12,37.8,37.9)]]
  (root/'history.jsonl').write_text('\n'.join(map(json.dumps,calls))+'\n')
  return fault,row.timestamp_ns(at(32)),row.timestamp_ns(at(34)),row.timestamp_ns(at(35))
 def test_scan_progress_requires_policy_calls_and_gap_coverage(self):
  with tempfile.TemporaryDirectory() as d:
   root=Path(d);self.fixture(root)
   def save(name,value):(root/name).write_text(json.dumps(value))
   save('start-scan-policy.json',dict(interval_nanos=100000000,budget=64))
   scan=dict(started='2026-10-03T00:00:36Z',finished='2026-10-03T00:00:37Z',kind='start',cursor=1,result=dict(next_sequence=65,inspected=64,reenqueued=1))
   save('start-scans.json',[scan])
   self.assertEqual(row.check_start_scan_progress(root)['gaps_with_progress'],1)
   for key,value in [('cursor',0),('finished','2026-10-03T00:00:35Z'),('kind','signal'),('result',dict(next_sequence=65,inspected=65,reenqueued=1)),('started','2026-10-03T00:00:39Z')]:
    bad=copy.deepcopy(scan);bad[key]=value;save('start-scans.json',[bad])
    with self.assertRaises(ValueError):row.check_start_scan_progress(root)
   save('start-scans.json',[])
   with self.assertRaises(ValueError):row.check_start_scan_progress(root)
   save('start-scans.json',[scan]);save('start-scan-policy.json',dict(interval_nanos=1000000000,budget=32))
   with self.assertRaises(ValueError):row.check_start_scan_progress(root)
 def test_requested_profiles_cannot_be_omitted_or_substituted(self):
  spec=importlib.util.spec_from_file_location('base_fixture',Path(__file__).with_name('test_tier3_journal_row.py'))
  base=importlib.util.module_from_spec(spec);spec.loader.exec_module(base)
  events=base.fixture('35s')
  for event in events:
   if 'Test' in event:event['Test']=row.TESTS['rolling_upgrade']
  events[0]['Output']=events[0]['Output'].replace('row=journal','row=rolling_upgrade')
  # Historic rows keep their own earlier scope, but cannot satisfy a new request.
  row.check(events,'35s','rolling_upgrade')
  with self.assertRaises(ValueError):row.check(events,'35s','rolling_upgrade',require_upgrade_start_gap=True)
  with self.assertRaises(ValueError):row.check(events,'35s','rolling_upgrade',expected_upgrade_shutdown='ldm')
  events[0]['Output']+='TIER3_UPGRADE_START_GAP=cohort-short-v1\nTIER3_UPGRADE_SHUTDOWN=sigkill\n'
  report=row.check(events,'35s','rolling_upgrade',require_upgrade_start_gap=True,expected_upgrade_shutdown='sigkill')
  self.assertEqual(report['upgrade_start_gap'],'cohort-short-v1')
  with self.assertRaises(ValueError):row.check(events,'35s','rolling_upgrade',expected_upgrade_shutdown='ldm')
  events[0]['Output']+='TIER3_UPGRADE_START_GAP=cohort-short-v1\n'
  with self.assertRaises(ValueError):row.check(events,'35s','rolling_upgrade',require_upgrade_start_gap=True)
 def test_complete_gap_spans_upgrade_with_real_repair_and_bound(self):
  with tempfile.TemporaryDirectory() as d:
   root=Path(d);row.check_upgrade_start_gap(root,*self.fixture(root))
 def test_missing_or_conflicting_proofs_rejected(self):
  for case in ('missing','changed_inv','early_release','late_terminal','missing_repair','unrelated_repair','no_kill','already_repaired','wrong_terminal','wrong_duplicate','invented_return','wrong_root'):
   with self.subTest(case=case),tempfile.TemporaryDirectory() as d:
    root=Path(d);args=list(self.fixture(root));fault=args[0];gap=fault['start_gap'];directory=root/gap['root']
    def change(path,mutate):
     data=json.loads(path.read_text());mutate(data);path.write_text(json.dumps(data))
    if case=='missing':directory.joinpath('killed-gap.json').unlink()
    elif case=='changed_inv':gap['after_upgrade']['Sequence']=13
    elif case=='early_release':gap['repair_released']='2026-10-03T00:00:33Z'
    elif case=='late_terminal':change(directory/'completion.json',lambda o:o.update(verified='2026-10-03T00:01:01.300000Z',kill_to_terminal_ns=30000000000))
    elif case=='missing_repair':(root/'repairs.json').write_text('[]')
    elif case=='unrelated_repair':change(root/'repairs.json',lambda o:o[0].update(id='other'))
    elif case=='no_kill':gap['proof']['signal']='SIGTERM'
    elif case=='already_repaired':gap['proof']['journal_absent']=False
    elif case=='wrong_terminal':change(directory/'terminal.json',lambda o:o.update(Subject='wf.jrn.matrixshort.other'))
    elif case in ('wrong_duplicate','invented_return'):
     calls=[json.loads(l) for l in (root/'history.jsonl').read_text().splitlines()]
     if case=='wrong_duplicate':calls[-1]['result']['inv_seq']=13
     else:calls[0]['return_ts']='2026-10-03T00:00:32Z'
     (root/'history.jsonl').write_text('\n'.join(map(json.dumps,calls)))
    elif case=='wrong_root':gap['root']='../outside'
    if case in ('changed_inv','early_release','no_kill','already_repaired'):(directory/'upgrade-gap.json').write_text(json.dumps(gap))
    if case in ('no_kill','already_repaired'):(directory/'killed-gap.json').write_text(json.dumps(gap['proof']))
    with self.assertRaises((ValueError,FileNotFoundError)):row.check_upgrade_start_gap(root,*args)
