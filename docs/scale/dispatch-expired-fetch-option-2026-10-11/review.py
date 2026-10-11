import hashlib,json
from pathlib import Path
out=Path(__file__).resolve().parent
repo=out.parents[2]
def sha(p):return hashlib.sha256(p.read_bytes()).hexdigest()
inputs=json.loads((out/'source-inputs.json').read_text())
for name,v in inputs.items():assert sha(repo/name)==v['sha256'],name
commands=json.loads((out/'commands.json').read_text())
assert len(commands)==4 and all(c['actual_exit']==0 for c in commands)
summary=[]
for mode in ['normal','race']:
 receipt=next(c for c in commands if c['mode']==mode and c['label']=='test')
 binary=Path(receipt['argv'][0])
 assert sha(binary)==receipt['executable']['sha256'] and binary.stat().st_size==receipt['executable']['bytes']
 assert not Path('/proc',str(receipt['pid'])).exists()
 lines=(out/(mode+'-test.log')).read_text().splitlines()
 assert lines[-1]=='PASS' and not any('--- FAIL:' in l for l in lines)
 passes=[l for l in lines if l.startswith('--- PASS:')]
 assert len(passes)==5,len(passes)
 for cut in ['expired=true','expired=false','deadline','cancel']:
  assert any('--- PASS:' in l and '/'+cut+' ' in l for l in lines),cut
 summary.append({'mode':mode,'top_level_passes':len(passes),'actual_exit':receipt['actual_exit'],'executable':receipt['executable']})
baseline=(out/'baseline.log').read_text()
assert 'handled=0 calls=1' in baseline and 'nats: invalid jetstream option: context deadline already exceeded' in baseline
(out/'review.json').write_text(json.dumps({'inputs':len(inputs),'runs':summary,'scope':'bounded dispatch controls; not full native campaign or seeded corpus'},indent=2)+'\n')
print('actual normal/race executables, exits, controls and unchanged inputs verified')
