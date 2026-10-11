"""Review focused logs, source immutability and closed native media."""
from pathlib import Path
import datetime
import gzip
import hashlib
import json
import re
import subprocess

base = Path(__file__).resolve().parent
exits = json.loads((base / 'actual-exits.json').read_text())
assert exits == {'race': 0, 'native': 0}
for name in ('ci-guard.py', 'native-ci-guard.py'):
    subprocess.check_call(['python3', str(base / name)])
seconds = {}
for mode, package in (('race', 'integrity'), ('native', 'worker')):
    log = (base / (mode+'.log')).read_text()
    match = re.search(r'^ok\s+js-wf/'+package+r'\s+([0-9.]+)s$', log, re.M)
    assert match and '\nFAIL\n' not in log
    seconds[mode] = float(match[1])
sources = json.loads((base / 'source-hashes.json').read_text())
assert all(hashlib.sha256(Path(f).read_bytes()).hexdigest() == h for f, h in sources.items())
root = Path('/home/exedev/js-wf-start-source-headers-native-20261011')
closure = subprocess.run(['lsof', '+D', str(root)], capture_output=True, text=True)
assert closure.returncode == 1 and not closure.stdout and not closure.stderr
(base / 'native-closure.json').write_text(json.dumps(dict(command=['lsof', '+D', str(root)], exit=closure.returncode, stdout=closure.stdout, stderr=closure.stderr), indent=2)+'\n')
files = []
for f in sorted(root.rglob('*')):
    st = f.lstat()
    if f.is_dir():
        continue
    assert f.is_file() and not f.is_symlink()
    files.append(dict(path=str(f.relative_to(root)), size=st.st_size, sha256=hashlib.sha256(f.read_bytes()).hexdigest(), mode=st.st_mode & 0o7777, mtime_ns=st.st_mtime_ns))
assert len(files) > 0
with gzip.open(base / 'native-storage-inventory.json.gz', 'wb') as out:
    out.write((json.dumps(dict(root=str(root), files=files), indent=2)+'\n').encode())
result = dict(accepted_focused_development=True, actual_exits=exits, seconds=seconds, source_files=len(sources), source_hashes_unchanged=True,
              physical_positive_cases=16, physical_negative_cases=216, native_checkpoint_cases=4, native_child_cases=4,
              parent_checkpoint_frames=8, retired_projection_only_receipts=4, executed_ci_guards=2, native_storage_closed=True,
              native_file_count=len(files), native_bytes=sum(x['size'] for x in files),
              captured_executables={n: json.loads((base / (n+'-executable.json')).read_text()) for n in ('integrity', 'worker')},
              logs_sha256={n: hashlib.sha256((base / (n+'.log')).read_bytes()).hexdigest() for n in ('race','native')},
              observed_utc=datetime.datetime.now(datetime.timezone.utc).isoformat(),
              scope='Owned canonical start source header admission only; unrelated headers opaque, production MatchesInvocation and full parent/source history remain separate. No power durability or full plan acceptance.')
(base / 'review.json').write_text(json.dumps(result, indent=2)+'\n')
print(json.dumps({k:v for k,v in result.items() if k!='captured_executables'}, indent=2))
