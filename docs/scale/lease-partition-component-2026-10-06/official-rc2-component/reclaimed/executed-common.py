from pathlib import Path
import sys, subprocess, json, hashlib, shutil, importlib.util, datetime, os
sys.dont_write_bytecode = True
repo = Path('/home/exedev/js-wf')
sys.path.insert(0,str(repo/'scripts'))
import fixture_archive
spec=importlib.util.spec_from_file_location('closed',repo/'scripts/verify-tier2-closed-originals.py')
closed=importlib.util.module_from_spec(spec); spec.loader.exec_module(closed)
def closure(root):
    limits=[]
    for p in Path('/proc').glob('[0-9]*'):
        try:
            assert not (p/'exe').resolve().is_relative_to(root)
            args=(p/'cmdline').read_bytes().split(b'\0')
            assert not any(str(root).encode() in arg for arg in args),(p,args)
            for link in ('cwd','root'):
                assert not (p/link).resolve().is_relative_to(root)
        except PermissionError: limits.append(str(p))
        except (FileNotFoundError,ProcessLookupError): pass
    ids=subprocess.check_output(['docker','ps','-aq'],text=True).split()
    containers=json.loads(subprocess.check_output(['docker','inspect',*ids],text=True)) if ids else []
    for c in containers:
        for m in c['Mounts']:
            source=Path(m['Source']).resolve()
            assert not source.is_relative_to(root) and not root.is_relative_to(source)
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
    return dict(unobservable_processes=limits,visible_descriptors=closed.verify_no_open_originals(root),
                docker_ids_including_stopped=ids,loopdevices=loops,mounts=mounts)
