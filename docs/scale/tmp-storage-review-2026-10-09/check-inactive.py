#!/usr/bin/env python3
"""Check process references and mount references before retiring this worktree."""
import json, os
from pathlib import Path
root='/tmp/js-wf-cas-baseline-topology'
blocked=[]; errors=[]
for p in Path('/proc').iterdir():
 if not p.name.isdigit() or int(p.name)==os.getpid(): continue
 try:
  refs=[]
  for n in ['cwd','exe','root']:
   try: refs.append(os.readlink(p/n))
   except FileNotFoundError: pass
  try:
   for f in (p/'fd').iterdir():
    try: refs.append(os.readlink(f))
    except FileNotFoundError: pass
  except FileNotFoundError: pass
  for n in ['maps','mountinfo']:
   try:
    if root in (p/n).read_text(): blocked.append({'pid':p.name,'reference':n})
   except (FileNotFoundError,ProcessLookupError): pass
  hits=[r for r in refs if r==root or r.startswith(root+'/')]
  if hits: blocked.append({'pid':p.name,'references':hits})
 except (FileNotFoundError,ProcessLookupError): pass
 except PermissionError as e: errors.append(str(e))
result={'root':root,'process_or_mount_references':blocked,'permission_errors':errors}
print(json.dumps(result,indent=2))
assert not blocked and not errors
