import copy,importlib.util,json,tempfile,unittest
from pathlib import Path
spec=importlib.util.spec_from_file_location('upgrade_guard',Path(__file__).with_name('check-tier3-journal-row.py'))
row=importlib.util.module_from_spec(spec);spec.loader.exec_module(row)
class UpgradeChecks(unittest.TestCase):
 def fixture(self,root):
  from datetime import datetime,timedelta,timezone
  base=datetime(2026,10,3,tzinfo=timezone.utc)
  def at(seconds):return (base+timedelta(seconds=seconds)).isoformat().replace('+00:00','Z')
  versions=['2.11.17']*5;faults=[]
  def proof(when,versions):
   config=lambda n:dict(name=n,num_replicas=5,storage='file')
   info=lambda n:dict(config=config(n),cluster=dict(leader='node0',replicas=[dict(name=f'node{i}',current=True) for i in range(1,5)]))
   return dict(version=3,health=[dict(node=i,url=f'http://127.0.0.1:{8200+i}/healthz?details=true',before=at(when-.95+i*.01),after=at(when-.94+i*.01),http_status=200,body='{"status":"ok"}') for i in range(5)],complete=True,phase='complete',started=at(when-1),backend_check=dict(started=at(when-.8),ended=at(when-.2),deadline=at(when+59),backend='fallback'),at=at(when),backend='fallback',run_info=info('WF_RUN'),timer_info=info('WF_TIMER'),nodes=[dict(node=i,version=v,server_id=f'id{i}',native_rejected='native timers require NATS 2.12+; connected server reports 2.11.17' if v=='2.11.17' else 'stream WF_RUN configuration mismatch: retained fallback') for i,v in enumerate(versions)])
  def save(n,o):(root/n).write_text(json.dumps(o))
  save('upgrade-initial.json',proof(0,versions))
  for i,node in enumerate([2,0,4,1,3],1):
   scheduled=30+(i-1)*135;save(f'fault-{i}-upgrade-before.json',proof(scheduled+1,versions));before=versions.copy();versions[node]='2.15.0';save(f'fault-{i}-upgrade-after.json',proof(scheduled+4,versions));faults.append(dict(node=node,scheduled=at(scheduled),killed=at(scheduled+2),healed=at(scheduled+5),versions_before=before,versions_after=versions.copy()))
  save('faults.json',faults)
 def test_five_actual_peer_boundaries_and_semantic_admission_required(self):
  with tempfile.TemporaryDirectory() as d:
   root=Path(d);self.fixture(root);r=row.check_upgrade_artifacts(root,dict(confirmed_faults=5,duration_seconds=600));self.assertEqual(r['upgraded_peers'],5);self.assertEqual(r['native_rejections'],55);self.assertTrue(r['full_five_peer_upgrade'])
 def test_missing_wrong_peer_native_fallback_and_readiness_rejected(self):
  for case in ('missing','wrong_version','duplicate_id','native_transport_error','native_allowed','backend','replica','duplicate_cut','versions','timestamp','partial','operation_error','deadline','late_proof','missing_schema','health_missing','health_unhealthy','health_wrong_peer','health_error','health_time'):
   with self.subTest(case=case),tempfile.TemporaryDirectory() as d:
    root=Path(d);self.fixture(root);p=root/'fault-5-upgrade-after.json';data=json.loads(p.read_text());faults=json.loads((root/'faults.json').read_text())
    if case=='missing':p.unlink()
    elif case=='wrong_version':data['nodes'][0]['version']='2.11.17'
    elif case=='duplicate_id':data['nodes'][0]['server_id']=data['nodes'][1]['server_id']
    elif case=='native_transport_error':data['nodes'][0]['native_rejected']='context deadline exceeded'
    elif case=='native_allowed':data['run_info']['config']['allow_msg_schedules']=True
    elif case=='backend':data['backend']='native'
    elif case=='replica':data['timer_info']['cluster']['replicas'][0]['current']=False
    elif case=='duplicate_cut':faults[-1]['node']=faults[0]['node']
    elif case=='versions':faults[-1]['versions_after']=['2.11.17']*5
    elif case=='health_missing':data['health'].pop()
    elif case=='health_unhealthy':data['health'][0]['http_status']=503
    elif case=='health_wrong_peer':data['health'][0]['node']=1
    elif case=='health_error':data['health'][0]['body']='{"status":"ok","errors":[{"error":"no quorum"}]}'
    elif case=='health_time':data['health'][0]['after']=data['backend_check']['deadline']
    elif case=='partial':data['complete']=False
    elif case=='operation_error':data['backend_check']['error']='context deadline exceeded'
    elif case=='deadline':data['backend_check']['deadline']=data['started']
    elif case=='late_proof':data['at']=data['backend_check']['deadline'][:-1]+'Z';data['backend_check']['deadline']=data['started']
    elif case=='missing_schema':data.pop('version')
    elif case=='timestamp':data['at']=faults[-1]['killed'][:-1]+'Z';data['at']=faults[-1]['scheduled']
    if case!='missing':p.write_text(json.dumps(data))
    (root/'faults.json').write_text(json.dumps(faults))
    with self.assertRaises((ValueError,FileNotFoundError)):row.check_upgrade_artifacts(root,dict(confirmed_faults=5,duration_seconds=600))
if __name__=='__main__':unittest.main()
