#!/usr/bin/env python3
import argparse,json,shutil,subprocess,tempfile
from pathlib import Path
p=argparse.ArgumentParser();p.add_argument('root',type=Path);p.add_argument('--repo',type=Path,required=True);p.add_argument('--output',type=Path,required=True);a=p.parse_args();root=a.root.resolve()
with tempfile.TemporaryDirectory() as d:
 relocated=Path(d)/'evidence';shutil.copytree(root,relocated)
 command=['python3',str(relocated/'review.py'),str(relocated),'--repo',str(a.repo.resolve())]
 positive=subprocess.run(command,capture_output=True,text=True);assert positive.returncode==0,positive.stderr
 results=[]
 def reject(name,file,change):
  path=relocated/file;original=path.read_bytes();change(path)
  result=subprocess.run(command,capture_output=True,text=True)
  path.write_bytes(original)
  assert result.returncode!=0,name
  results.append(dict(case=name,rejected=True,error=result.stderr.splitlines()[-1]))
 def mutate_json(path,edit):
  value=json.loads(path.read_text());edit(value);path.write_text(json.dumps(value))
 reject('wrong_source_hash','source-before.json',lambda p:mutate_json(p,lambda v:v['files'].__setitem__('go.mod','0'*64)))
 reject('wrong_binary_hash','binary.json',lambda p:mutate_json(p,lambda v:v.__setitem__('binary_sha256','0'*64)))
 reject('missing_pin','tier1-regression-inventory.txt',lambda p:p.write_text('\n'.join(p.read_text().splitlines()[1:])+'\n'))
 reject('substituted_seed_count','binary.json',lambda p:mutate_json(p,lambda v:v['environment'].__setitem__('SIM_SEEDS','10000')))
 output=dict(relocated_positive=True,negative_controls=results)
 a.output.write_text(json.dumps(output,indent=2)+'\n')
