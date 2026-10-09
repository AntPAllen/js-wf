"""Compile and execute one retained native race binary from a clean checkout."""
import datetime
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys

checkout = Path(sys.argv[1]).resolve()
base = Path(__file__).resolve().parent
artifact = Path('/home/exedev/js-wf-reader-refresh-native-20261009')
assert not artifact.exists()
assert not subprocess.check_output(['git', 'status', '--porcelain'], cwd=checkout)
revision = subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd=checkout, text=True).strip()
names = subprocess.check_output(['git', 'ls-files'], cwd=checkout, text=True).splitlines()
def inventory():
    return {name: hashlib.sha256((checkout / name).read_bytes()).hexdigest()
            for name in names if (checkout / name).is_file() and
            (name.endswith('.go') or name in ('go.mod', 'go.sum') or name.startswith('sim/testdata/'))}
def stamp():
    return datetime.datetime.now(datetime.timezone.utc).isoformat()
artifact.mkdir()
before = inventory()
(base / 'native-source-before.json').write_text(json.dumps(dict(revision=revision, files=before), indent=2) + '\n')
env = dict(os.environ, GOMAXPROCS='4', GOMEMLIMIT='512MiB')
binary = artifact / 'worker-race.test'
record = dict(revision=revision, checkout=str(checkout), artifact=str(artifact), started=stamp(),
              env={name: env[name] for name in ('GOMAXPROCS', 'GOMEMLIMIT')}, commands=[])
def save():
    (base / 'native-command-results.json').write_text(json.dumps(record, indent=2) + '\n')
def call(command, log):
    event = dict(command=list(map(str, command)), started=stamp(), working_directory=str(checkout))
    record['commands'].append(event)
    save()
    with (base / log).open('w') as output:
        child = subprocess.run(event['command'], cwd=checkout, env=env, stdout=output, stderr=subprocess.STDOUT)
    event.update(exit=child.returncode, finished=stamp())
    save()
    return child.returncode
compile_exit = call(['go', 'test', '-race', '-c', './worker', '-o', binary], 'native-compile.log')
if compile_exit:
    raise SystemExit(compile_exit)
record['binary_sha256'] = hashlib.sha256(binary.read_bytes()).hexdigest()
record['binary_bytes'] = binary.stat().st_size
metadata = subprocess.check_output(['go', 'version', '-m', binary], cwd=checkout, text=True)
assert '-race=true' in metadata
(base / 'native-binary-build.txt').write_text(metadata)
assert inventory() == before
assert not subprocess.check_output(['git', 'status', '--porcelain'], cwd=checkout)
save()
run_exit = call([binary, '-test.run=^TestNativeGraphCheckpointMaterializedReferences/R3Domain$',
                 '-test.count=1', '-test.timeout=3m', '-test.v'], 'native-r3-race.log')
after = inventory()
assert before == after
assert not subprocess.check_output(['git', 'status', '--porcelain'], cwd=checkout)
record.update(finished=stamp(), actual_test_exit=run_exit, source_unchanged=True)
save()
(base / 'native-source-after.json').write_text(json.dumps(dict(revision=revision, files=after), indent=2) + '\n')
print(json.dumps(dict(revision=revision, actual_test_exit=run_exit, source_unchanged=True)))
raise SystemExit(run_exit)
