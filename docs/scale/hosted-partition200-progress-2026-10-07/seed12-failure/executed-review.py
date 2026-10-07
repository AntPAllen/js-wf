import sys
sys.dont_write_bytecode=True
from pathlib import Path
import json,hashlib,zipfile,subprocess,shutil,datetime
repo=Path('/home/exedev/js-wf');root=Path('/tmp/js-wf-hosted-partition-seed12-failure-20261007');out=repo/'docs/scale/hosted-partition200-progress-2026-10-07/seed12-failure';root.mkdir();out.mkdir(parents=True)
def sha(p):return hashlib.sha256(p.read_bytes()).hexdigest()
for src,name in [('/tmp/hosted-partition-seed12-failure.zip','native-artifact.zip'),('/tmp/hosted-partition-seed12-source.zip','source-artifact.zip'),('/tmp/hosted-partition-seed12-failure.log','provider-job.log'),('/tmp/hosted-partition200-current-jobs.json','provider-jobs.jsonl')]:shutil.copyfile(src,root/name)
assert sha(root/'native-artifact.zip')=='a7106515f4e9638c90065e36b190eef0c6539067e99bab6ef466f0f175642032'
files={}
for file,destination in [('native-artifact.zip','native'),('source-artifact.zip','source')]:
 with zipfile.ZipFile(root/file) as archive:
  assert archive.testzip() is None
  names=archive.namelist();assert len(names)==len(set(names))
  for info in archive.infolist():
   name=Path(info.filename);assert len(name.parts)==1 and name.name not in ('.','..') and not info.is_dir()
   data=archive.read(info);target=root/destination/name;target.parent.mkdir(exist_ok=True);target.write_bytes(data)
   files[destination+'/'+str(name)]={'bytes':len(data),'sha256':hashlib.sha256(data).hexdigest()}
source=json.loads((root/'source/workload-source.json').read_text());revision=source['revision'];assert revision=='d9093d74f4e18562d70173c0fc6715c394a0314a'
inputs=source['inputs'];refs=[revision+':'+name for name in inputs]
data=subprocess.check_output(['git','cat-file','--batch'],input=('\n'.join(refs)+'\n').encode(),cwd=repo);pos=0
for name,record in inputs.items():
 end=data.index(b'\n',pos);header=data[pos:end].split();assert header[1]==b'blob';size=int(header[2]);pos=end+1;blob=data[pos:pos+size];pos+=size+1
 assert {'bytes':size,'sha256':hashlib.sha256(blob).hexdigest()}==record,name
assert pos==len(data)
events=[json.loads(x) for x in (root/'native/matrix-partition-12-test.jsonl').read_text().splitlines()]
test='TestMixedMatrixServerPartitionEveryThirtySeconds';terminal=[x for x in events if x.get('Test')==test and x['Action'] in ('pass','fail')];assert len(terminal)==1 and terminal[0]['Action']=='fail'
log=''.join(e.get('Output','') for e in events);assert 'workflow replica recovery stream=KV_WF_LEASE' in log and '"lag":3688' in log
assert 'MATRIX_RESULT' not in log
faults=json.loads((root/'native/matrix-partition-12-faults.json').read_text());shutil.copyfile(root/'native/matrix-partition-12-faults.json',out/'faults.json')
review={'run':37500390198,'job':112465583274,'seed':12,'source':revision,'native_artifact_id':11470794502,'source_artifact_id':11471520036,'native_artifact_sha256':sha(root/'native-artifact.zip'),'native_artifact_bytes':(root/'native-artifact.zip').stat().st_size,'source_artifact_sha256':sha(root/'source-artifact.zip'),'zip_crc_and_all_member_bytes_read':True,'members':files,'captured_source_inputs_match_all_recorded_git_blobs':len(inputs),'native_terminal':terminal[0],'accepted':False,'cause_confirmed':False,'no_completed_matrix_result':True,'scope':'Original hosted seed12 failure; full raw logs/fault/history/operation/source evidence preserved. No physical stores or actual hosted SDK/server executables were provided by these artifacts. Raft warnings and replica lag do not prove the historical cause.'}
(root/'review.json').write_text(json.dumps(review,indent=2)+'\n');shutil.copyfile(root/'review.json',out/'review.json');shutil.copyfile(__file__,root/'executed-review.py');shutil.copyfile(__file__,out/'executed-review.py')
sys.path.insert(0,str(repo/'scripts'));import fixture_archive
fixture_archive.capture(root,Path('/tmp/js-wf-hosted-partition-seed12-failure-20261007.tar.gz'),out)
print('HOSTED_FAILURE_ALL_MEMBERS_AND_SOURCE_INPUTS_VERIFIED',len(inputs),terminal[0]['Elapsed'])
