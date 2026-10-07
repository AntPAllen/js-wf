import sys,pathlib,json,subprocess,shutil,datetime
sys.dont_write_bytecode=True;sys.path.insert(0,'/tmp')
from storage_review_common import closure,fixture_archive
repo=pathlib.Path('/home/exedev/js-wf');out=repo/'docs/scale/blob-native-authority-2026-10-07/reclaimed'
items=[(pathlib.Path('/tmp/js-wf-native-blob-authority-v2-20261007'),repo/'docs/scale/blob-native-authority-2026-10-07/accepted'),(pathlib.Path('/tmp/js-wf-native-blob-authority-20261007'),repo/'docs/scale/blob-native-authority-2026-10-07/committed-reopen-failure')]
report={'started_utc':datetime.datetime.now(datetime.timezone.utc).isoformat(),'removed':[],'scope':'Complete successful and failed native authority fixtures preserved unchanged; candidate campaign untouched.'}
paths=[]
for root,canonical in items:
 meta=json.loads((canonical/'archive-verification.json').read_text());manifest=json.loads((canonical/'fixture-inventory.json').read_text());receipt=json.loads((canonical/'s3-readback.json').read_text());expected={'bytes':meta['archive_bytes'],'sha256':meta['archive_sha256']};archive=root.with_suffix('.tar.gz');proof=root.with_name(root.name+'-proof')
 assert fixture_archive.inventory(root)==manifest['files']
 with archive.open('rb') as stream:assert fixture_archive.verify_hashed_stream(stream,expected)[0]==manifest
 with subprocess.Popen(['curl','--config','-','--aws-sigv4','aws:amz:us-east-1:s3','--silent','--show-error','--fail','--connect-timeout','30','--max-time','1800',receipt['archive']['url']],stdin=subprocess.PIPE,stdout=subprocess.PIPE,stderr=subprocess.PIPE) as p:
  p.stdin.write(b'user = "x:x"\n');p.stdin.close();remote,digest=fixture_archive.verify_hashed_stream(p.stdout,expected);error=p.stderr.read();assert p.wait()==0,error
 assert remote==manifest
 for p in proof.iterdir():assert p.read_bytes()==(canonical/p.name).read_bytes()
 paths.extend([root,archive,proof]);report['removed'].append({'root':str(root),'canonical':str(canonical.relative_to(repo)),'remote_digest':digest,'verified_members':meta['members'],'closure':{str(p):closure(p) for p in [root,archive,proof]}})
for root,canonical in items:assert fixture_archive.inventory(root)==json.loads((canonical/'fixture-inventory.json').read_text())['files']
allocated=0
for p in paths:
 if p.is_file():allocated+=p.stat().st_blocks*512;p.unlink()
 else:
  allocated+=sum(f.stat().st_blocks*512 for f in p.rglob('*') if f.is_file());shutil.rmtree(p)
report['reclaimed_allocated_bytes']=allocated;report['finished_utc']=datetime.datetime.now(datetime.timezone.utc).isoformat();report['filesystem']=subprocess.check_output(['df','-h','/tmp'],text=True)
(out/'cleanup.json').write_text(json.dumps(report,indent=2)+'\n');print(json.dumps({'reclaimed_allocated_bytes':allocated}),flush=True)
