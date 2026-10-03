#!/usr/bin/env python3
"""Relocate accepted originals and reject semantically false, consistently hashed cases."""
import argparse,copy,hashlib,importlib.util,io,json,shutil,subprocess,tarfile,tempfile
from pathlib import Path
p=argparse.ArgumentParser();p.add_argument('--root',type=Path,required=True);p.add_argument('--repo',type=Path,required=True);p.add_argument('--reviewer',type=Path,required=True);p.add_argument('--output',type=Path,required=True);a=p.parse_args()
spec=importlib.util.spec_from_file_location('reviewer',a.reviewer);reviewer=importlib.util.module_from_spec(spec);spec.loader.exec_module(reviewer)
original=a.root/'originals.tar.gz';manifest=json.loads((a.root/'archive-manifest.json').read_text());original_sha=reviewer.sha(original)
with tempfile.TemporaryDirectory() as d:
 root=Path(d);shutil.copyfile(original,root/'originals.tar.gz');shutil.copyfile(a.root/'archive-manifest.json',root/'archive-manifest.json')
 accepted=reviewer.review(root,a.repo);assert accepted['accepted_row']
 def rewrite(case):
  changed=copy.deepcopy(manifest)
  temporary=root/'case.tmp.tar.gz'
  with tarfile.open(original) as source,tarfile.open(temporary,'w:gz') as output:
   for member in source.getmembers():
    if case=='missing_compiled_source' and member.name=='source/worker/worker.go':
     del changed['files'][member.name];continue
    replacements={'failed_status':{'execution.json'},'substituted_seed':{'execution.json'},'false_normal_binary':{'execution.json','binary.json','commands.json'},'short_named_elapsed':{'events.jsonl'}}
    if member.name in replacements.get(case,set()):
     raw=source.extractfile(member).read()
     if member.name=='events.jsonl':
      events=[json.loads(line) for line in raw.splitlines()]
      for event in events:
       if event.get('Action')=='pass' and event.get('Test')=='TestFiveContainerMixedJournalLeaderEveryThirtySeconds':event['Elapsed']=1
      raw=('\n'.join(map(json.dumps,events))+'\n').encode()
     else:
      value=json.loads(raw)
      if case=='failed_status':value['status']='failed'
      elif case=='substituted_seed':value['seed']=2
      elif member.name in ('execution.json','binary.json'):value['race']=False
      else:
       for command in value:
        if '-race' in command['command']:command['command'].remove('-race')
      raw=(json.dumps(value,indent=2)+'\n').encode()
     info=copy.copy(member);info.size=len(raw);output.addfile(info,io.BytesIO(raw));changed['files'][member.name]=hashlib.sha256(raw).hexdigest()
    else:output.addfile(member,source.extractfile(member))
  temporary.rename(root/'originals.tar.gz');changed['archive_sha256']=reviewer.sha(root/'originals.tar.gz')
  (root/'archive-manifest.json').write_text(json.dumps(changed,indent=2)+'\n')
 results=[]
 for case in ('failed_status','missing_compiled_source','false_normal_binary','substituted_seed','short_named_elapsed'):
  rewrite(case)
  try:reviewer.review(root,a.repo)
  except (ValueError,subprocess.CalledProcessError) as error:
   results.append(dict(case=case,rejected=True,error=str(error)));continue
  raise AssertionError('accepted negative control:'+case)
 assert reviewer.sha(original)==original_sha
 a.output.write_text(json.dumps(dict(relocated_positive=True,negative_controls=results,original_archive_unchanged=True),indent=2)+'\n')
