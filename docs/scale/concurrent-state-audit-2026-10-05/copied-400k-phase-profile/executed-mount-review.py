from pathlib import Path
import json,hashlib
root=Path('/tmp/js-wf-400k-copied-capacity-profile-20261005');out=Path('/home/exedev/js-wf/docs/scale/concurrent-state-audit-2026-10-05/copied-400k-phase-profile')
servers=json.loads((root/'actual-containers/actual-servers.json').read_text());nodes=set();records=[]
with (root/'originals/TestConcurrentStateR5CopiedCapacityProfile/cluster/nats-server').open('rb') as f:digest=hashlib.file_digest(f,'sha256').hexdigest()
for x in servers:
 node=int(x['container']['Name'].rsplit('-n',1)[-1]);nodes.add(node)
 mounts=[m for m in x['container']['Mounts'] if m['Destination']=='/data'];assert len(mounts)==1
 assert mounts[0]['Source']==str(root/'copied-stores'/f'node-{node}')
 assert x['sha256']==digest and '\tmod\tgithub.com/nats-io/nats-server/v2\tv2.15.0' in x['actual_proc_build_info']
 assert not Path('/proc/'+str(x['host_pid'])).exists()
 records.append({'node':node,'host_pid':x['host_pid'],'sha256':digest,'data_mount':mounts[0]['Source']})
assert nodes==set(range(5))
(out/'mount-review.json').write_text(json.dumps({'all_five_logical_nodes_verified':True,'mounts_only_disposable_copies':True,'server_bytes_match_retained_fixture_binary':True,'all_observed_processes_closed':True,'servers':records},indent=2)+'\n')
(out/'executed-mount-review.py').write_bytes(Path(__file__).read_bytes())
