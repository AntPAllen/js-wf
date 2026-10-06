from pathlib import Path
import sys,subprocess,json,hashlib,shutil,importlib.util,datetime,os
repo=Path('/home/exedev/js-wf');sys.path.insert(0,str(repo/'scripts'));import fixture_archive
spec=importlib.util.spec_from_file_location('closed',repo/'scripts/verify-tier2-closed-originals.py');closed=importlib.util.module_from_spec(spec);spec.loader.exec_module(closed)
spec=importlib.util.spec_from_file_location('s3',repo/'scripts/offload-proof-to-s3.py');s3=importlib.util.module_from_spec(spec);spec.loader.exec_module(s3)
def closure(root):
 limits=[]
 for p in Path('/proc').glob('[0-9]*'):
  try:
   exe=(p/'exe').resolve();args=(p/'cmdline').read_bytes().split(b'\0')
   assert not exe.is_relative_to(root)
   for link in ["cwd", "root"]:
    assert not (p/link).resolve().is_relative_to(root)
   assert not any(str(root).encode() in arg for arg in args),(p,args)
  except PermissionError:limits.append(str(p))
  except (FileNotFoundError,ProcessLookupError):pass
 ids=subprocess.check_output(['docker','ps','-q'],text=True).split()
 running=json.loads(subprocess.check_output(['docker','inspect',*ids],text=True)) if ids else []
 for c in running:
  for m in c['Mounts']:
   source=Path(m['Source']).resolve();assert not source.is_relative_to(root) and not root.is_relative_to(source)
 loops=json.loads(subprocess.check_output(['sudo','-n','losetup','--list','--json'],text=True))
 for device in loops['loopdevices']:
  assert not Path(device['back-file']).resolve().is_relative_to(root)
 mounts=json.loads(subprocess.check_output(['findmnt','--json','--output','TARGET,SOURCE'],text=True))
 def walk(rows):
  for row in rows:
   assert not Path(row['target']).resolve().is_relative_to(root)
   assert not row['source'].startswith(str(root))
   walk(row.get('children',[]))
 walk(mounts['filesystems'])
 return dict(unobservable_processes=limits,visible_descriptors=closed.verify_no_open_originals(root),loopdevices=loops,mounts=mounts,running_docker_ids=ids)


root=Path('/tmp/js-wf-local-partition200-20261006/seed-006')
out=repo/'docs/scale/local-tier2-partition-2026-10-06/seed-006-failure'
raw=Path('/tmp/js-wf-local-partition200-seed006-failure-20261006.tar.gz')
initial=closure(root)
proof=fixture_archive.capture(root,raw,out,compresslevel=1)
final=closure(root)
for name in ['native.log','execution.json','commands.json','acceptance.json','source-before.json','source-after.json','external-source-before.json','external-source-after.json']:
 shutil.copyfile(root/name,out/name)
shutil.copyfile(root.parent/'campaign.json',out/'campaign-checkpoint.json')
unit=dict(l.split('=',1) for l in subprocess.check_output(['systemctl','--user','show','js-wf-local-partition200-20261006.service','-p','ActiveState','-p','MainPID','-p','ExecMainStatus'],text=True).splitlines())
assert unit==dict(ActiveState='failed',MainPID='0',ExecMainStatus='1')
execution=json.loads((root/'execution.json').read_text());assert execution['exit_code']==1
report=dict(root=str(root),unit=unit,closure_before=initial,closure_after=final,complete_archive=proof,scope='Full failed seed6 retention and terminal campaign checkpoint. Five prior native exit0 seeds do not qualify the200 row. Replica recovery deadline failed with KV_WF_LEASE replica lag3535; cause unconfirmed. No broker opened, repaired, timeout changed or campaign restarted.')
(out/'retention.json').write_text(json.dumps(report,indent=2)+'\n')
shutil.copyfile(__file__,out/'executed-retention.py')
print(json.dumps(proof))
