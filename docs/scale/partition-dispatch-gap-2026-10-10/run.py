"""One frozen 35-second seed2 diagnostic; never promotes the full row."""
import datetime
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import subprocess
import sys
import tarfile

repo = Path('/home/exedev/js-wf')
checkout = Path('/home/exedev/js-wf-partition-dispatch-gap-qualification')
root = Path('/home/exedev/js-wf-partition-dispatch-gap-20261010')
source = sys.argv[1]
assert subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd=checkout, text=True).strip() == source
assert not subprocess.check_output(['git', 'status', '--porcelain'], cwd=checkout)
root.mkdir()  # A fresh attempt cannot overwrite an earlier outcome.
sys.path.insert(0, str(checkout / 'scripts'))
import fixture_archive
spec = importlib.util.spec_from_file_location('s3', checkout / 'scripts/offload-proof-to-s3.py')
s3 = importlib.util.module_from_spec(spec)
spec.loader.exec_module(s3)

def stamp():
    return datetime.datetime.now(datetime.timezone.utc).isoformat()

def digest(path):
    with path.open('rb') as stream:
        return s3.digest(stream)

names = subprocess.check_output(['git', 'ls-files'], cwd=checkout, text=True).splitlines()
names = [n for n in names if n.endswith(('.go', '.py', '.yml')) or n in ('go.mod', 'go.sum') or n.startswith('sim/testdata/')]
def inventory():
    return {'source': source, 'files': {n: digest(checkout / n) for n in names}}

before = inventory()
(root / 'source-before.json').write_text(json.dumps(before, indent=2) + '\n')
env = dict(os.environ, GOMAXPROCS='2', GOMEMLIMIT='2GiB', WF_MATRIX_CHAOS='1',
           WF_MATRIX_DURATION='35s', FAULT_SEED='2', WF_MATRIX_OPERATION_TIMINGS='1',
           WF_MATRIX_PARTITION_TIMINGS='1', MATRIX_ARTIFACT_PREFIX=str(root / 'matrix'),
           WF_MATRIX_PROCESS_ROOT=str(root / 'stores'),
           WF_MATRIX_PARTITION_SERVER_BIN=str(root / 'candidate-server'))
keys = ['GOMAXPROCS', 'GOMEMLIMIT', 'WF_MATRIX_CHAOS', 'WF_MATRIX_DURATION', 'FAULT_SEED',
        'WF_MATRIX_OPERATION_TIMINGS', 'WF_MATRIX_PARTITION_TIMINGS', 'MATRIX_ARTIFACT_PREFIX',
        'WF_MATRIX_PROCESS_ROOT', 'WF_MATRIX_PARTITION_SERVER_BIN']
state = dict(source=source, checkout=str(checkout), root=str(root), pid=os.getpid(),
             invocation=os.environ.get('INVOCATION_ID'), started=stamp(), commands=[],
             environment={k: env[k] for k in keys}, accepted=False,
             scope='One short diagnostic on a shared VM, exact original candidate binary, current worker. Not original seed2 duration/source or full200 acceptance.')
def save():
    (root / 'state.json').write_text(json.dumps(state, indent=2) + '\n')
def run(args, log):
    row = dict(command=args, started=stamp(), cwd=str(checkout))
    state['commands'].append(row)
    save()
    with (root / log).open('wb') as output:
        row['exit'] = subprocess.call(args, cwd=checkout, env=env, stdout=output, stderr=subprocess.STDOUT)
    row['finished'] = stamp()
    save()
    return row['exit']

save()
try:
    proof = repo / 'docs/scale/lease-partition-component-2026-10-06/contiguous-component'
    meta = json.loads((proof / 'archive-verification.json').read_text())
    manifest = json.loads((proof / 'fixture-inventory.json').read_text())
    receipt = json.loads((proof / 's3-readback.json').read_text())
    assert digest(proof / 'fixture-inventory.json')['sha256'] == meta['inventory_sha256']
    expected = dict(bytes=meta['archive_bytes'], sha256=meta['archive_sha256'])
    assert receipt['archive']['full_readback'] == expected
    archive = root / 'candidate-proof.tar.gz'
    with archive.open('xb') as output:
        fetch = subprocess.run(['curl', '--config', '-', '--aws-sigv4', 'aws:amz:us-east-1:s3',
                                '--silent', '--show-error', '--fail', '--connect-timeout', '30',
                                '--max-time', '600', receipt['archive']['url']],
                               input=s3.credentials(), stdout=output, stderr=subprocess.PIPE)
    assert fetch.returncode == 0, 'Candidate restore download failed'
    with archive.open('rb') as stream:
        declared, actual = fixture_archive.verify_hashed_stream(stream, expected)
    assert declared == manifest
    with tarfile.open(archive) as contents:
        member = contents.getmember('candidate-server')
        assert member.isfile()
        data = contents.extractfile(member).read()
    binary = root / 'candidate-server'
    binary.write_bytes(data)
    binary.chmod(0o755)
    assert digest(binary) == {k: manifest['files']['candidate-server'][k] for k in ('bytes', 'sha256')}
    assert digest(binary)['sha256'] == 'a56911eef56974eb10eb8b749a46b618100e8662b524f44be37ba06f2de8ad68'
    state['candidate_restore'] = dict(url=receipt['archive']['url'], archive=actual, binary=digest(binary), restored_members=['candidate-server'])
    save()
    assert run(['go', 'test', '-c', '-o', str(root / 'integration.test'), './integration'], 'compile.log') == 0
    state['sdk_binary'] = digest(root / 'integration.test')
    run(['go', 'version', '-m', str(root / 'integration.test')], 'sdk-build-info.log')
    run(['go', 'version', '-m', str(binary)], 'candidate-build-info.log')
    code = run(['go', 'tool', 'test2json', '-t', '-p', 'js-wf/integration', str(root / 'integration.test'),
                '-test.v=test2json', '-test.run=^TestMixedMatrixServerPartitionEveryThirtySeconds$',
                '-test.count=1', '-test.timeout=8m'], 'events.jsonl')
    state['native_exit'] = code
    state['phase'] = 'closed'
finally:
    after = inventory()
    (root / 'source-after.json').write_text(json.dumps(after, indent=2) + '\n')
    state['sources_unchanged'] = before == after
    state['finished'] = stamp()
    save()
assert state['sources_unchanged']
sys.exit(state['native_exit'])
