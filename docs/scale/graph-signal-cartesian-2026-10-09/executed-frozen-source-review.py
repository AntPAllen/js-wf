import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess

base = Path(__file__).resolve().parent
repo = base.parents[2]
manifest = json.loads((base / 'source-before.json').read_text())
revision = subprocess.check_output(['git', 'rev-parse', '4f1f2b2'], cwd=repo, text=True).strip()
process = subprocess.Popen(['git', 'cat-file', '--batch'], cwd=repo, stdin=subprocess.PIPE, stdout=subprocess.PIPE)
try:
    for name, digest in manifest['files'].items():
        process.stdin.write((revision + ':' + name + '\n').encode())
        process.stdin.flush()
        header = process.stdout.readline().split()
        assert len(header) == 3 and header[1] == b'blob', name
        content = process.stdout.read(int(header[2]))
        assert process.stdout.read(1) == b'\n'
        assert hashlib.sha256(content).hexdigest() == digest, name
finally:
    process.stdin.close()
    assert process.wait() == 0
review = {'source_revision': revision, 'repository_inputs_git_verified': len(manifest['files']), 'manifest_sha256': hashlib.sha256((base / 'source-before.json').read_bytes()).hexdigest(), 'scope': 'Exact observed Cartesian source matches the committed 4f1f2b2 image; subsequent native-authority model additions are excluded.'}
(base / 'frozen-source-review.json').write_text(json.dumps(review, indent=2) + '\n')
for proc in Path('/proc').iterdir():
    if not proc.name.isdigit():
        continue
    try:
        args = (proc / 'cmdline').read_bytes().split(b'\0')
        if not args[0].endswith(b'/sim.test') or b'-test.run=^TestGraphSignalRuntimeCartesianReplay$' not in args:
            continue
        original = Path(os.readlink(proc / 'exe'))
        retained = repo.parent / 'js-wf-signal-cartesian-race-binary-20261009'
        retained.mkdir(exist_ok=True)
        binary = retained / 'sim.test'
        shutil.copy2(original, binary)
        build = subprocess.check_output(['go', 'version', '-m', str(binary)], text=True)
        assert '-race=true' in build
        assert os.readlink(proc / 'cwd') == str(repo / 'sim')
        record = {'pid': int(proc.name), 'arguments': [arg.decode() for arg in args if arg], 'working_directory': str(repo / 'sim'), 'retained_binary': str(binary), 'binary_sha256': hashlib.sha256(binary.read_bytes()).hexdigest(), 'build_info': build, 'source_revision': revision, 'repository_inputs_git_verified': len(manifest['files'])}
        (base / 'race-binary.json').write_text(json.dumps(record, indent=2) + '\n')
        break
    except (FileNotFoundError, PermissionError, ProcessLookupError):
        continue
else:
    raise AssertionError('No live Cartesian race test binary found')
print(json.dumps(review))
