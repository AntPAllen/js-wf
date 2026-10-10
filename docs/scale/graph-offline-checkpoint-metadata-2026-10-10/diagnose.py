"""Preserve old-reader controls using a real exported two-checkpoint bundle."""
import base64,copy,hashlib,json,os,subprocess
from pathlib import Path
base=Path(__file__).resolve().parent
root=Path('/home/exedev/js-wf-offline-checkpoint-metadata-diagnostic-20261010');root.mkdir(exist_ok=True)
source=Path('/home/exedev/js-wf-child-offline-20261010/artifacts/offline-2667111011/completed.json')
bundle=json.loads(source.read_text())
cli=Path('/home/exedev/js-wf-selected-child-20261010/wf');plugin=cli.parent/'handler.so'
symbol='ChildWorkflow' if isinstance(json.loads(base64.b64decode(bundle['input'])),dict) else 'Workflow'
def hash(data):return hashlib.sha256(data).hexdigest()
report=dict(reader_source='02c3506b76b326237c00987f6f31994f833fe0b9',source_bundle=str(source),source_bundle_sha256=hash(source.read_bytes()),cli=str(cli),cli_sha256=hash(cli.read_bytes()),plugin=str(plugin),plugin_sha256=hash(plugin.read_bytes()),symbol=symbol,rows=[])
for name in ('control','missing-metadata','changed-metadata-bytes','wrong-metadata-hash','rehash-wrong-identity','rehash-wrong-anchor','rehash-unknown-version'):
 b=copy.deepcopy(bundle);entry=next(r for r in b['journal'] if isinstance(r.get('payload'),dict) and r['payload'].get('checkpoint_metadata_ref'));e=entry['payload'];ref=e['checkpoint_metadata_ref']
 raw=base64.b64decode(b['objects'][ref])
 if name=='missing-metadata':del b['objects'][ref]
 elif name=='changed-metadata-bytes':b['objects'][ref]=base64.b64encode(raw+b' ').decode()
 elif name=='wrong-metadata-hash':e['checkpoint_metadata_hash']='0'*64
 elif name.startswith('rehash-'):
  meta=json.loads(raw)
  if name=='rehash-wrong-identity':meta['identity']['id']='foreign'
  elif name=='rehash-wrong-anchor':meta['anchor']['index']+=1
  else:meta['version']=99
  raw=json.dumps(meta,separators=(',',':')).encode();b['objects'][ref]=base64.b64encode(raw).decode();e['checkpoint_metadata_hash']=hash(raw)
 path=root/(name+'.json');path.write_text(json.dumps(b)+'\n')
 args=[str(cli),'-url','nats://127.0.0.1:1','-handler-plugin',str(plugin),'-handler-symbol',symbol,'-replay-bundle',str(path),'replay']
 result=subprocess.run(args,env=dict(os.environ,WF_REPLAY_EFFECT_MARKER=str(root/'effect')),stdout=subprocess.PIPE,stderr=subprocess.STDOUT,timeout=20)
 report['rows'].append(dict(name=name,command=args,input_sha256=hash(path.read_bytes()),exit=result.returncode,output=result.stdout.decode()))
 assert result.returncode==0 and json.loads(result.stdout)['result']==60
 assert not (root/'effect').exists()
(root/'report.json').write_text(json.dumps(report,indent=2)+'\n')
(base/'old-diagnostic.json').write_text(json.dumps(report,indent=2)+'\n')
print('old reader accepted valid control and all six invalid metadata variants')
