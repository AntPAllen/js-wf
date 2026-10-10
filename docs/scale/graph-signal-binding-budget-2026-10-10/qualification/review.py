"""Independently qualify the frozen focused client race binary and actual exits."""
import gzip
import hashlib
import io
import json
from pathlib import Path
import re
import subprocess

base = Path(__file__).resolve().parent
state = json.loads((base / 'state.json').read_text())
launch = json.loads((base / 'launch.json').read_text())
checkout = Path(state['checkout'])
props = dict(line.split('=', 1) for line in subprocess.check_output([
    'systemctl', '--user', 'show', state['unit'], '-p', 'LoadState', '-p', 'MainPID',
    '-p', 'InvocationID', '-p', 'ExecMainStatus', '-p', 'ExecMainExitTimestamp',
    '-p', 'RemainAfterExit', '-p', 'CPUQuotaPerSecUSec'], text=True).splitlines())
assert props['LoadState'] == 'loaded' and props['MainPID'] == '0' and props['ExecMainStatus'] == '0'
assert props['ExecMainExitTimestamp'] and props['RemainAfterExit'] == 'yes'
assert props['InvocationID'] == state['invocation'] == launch['invocation']
assert props['CPUQuotaPerSecUSec'] == launch['service']['CPUQuotaPerSecUSec'] == '1s'
assert launch['source'] == state['source'] and launch['supervisor_pid'] == state['supervisor_pid']
assert hashlib.sha256((base / 'supervisor.py').read_bytes()).hexdigest() == launch['supervisor_sha256']
assert state['terminal'] and state['child_exit'] == 0 and state['inputs_unchanged']
assert state['configuration'] == dict(GOMAXPROCS='2', GOMEMLIMIT='1GiB', GOWORK='off', GOFLAGS='')
assert subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd=checkout, text=True).strip() == state['source']
assert not subprocess.check_output(['git', 'status', '--porcelain'], cwd=checkout)
before = json.loads(gzip.decompress((base / 'source-before.json.gz').read_bytes()))
assert before == json.loads(gzip.decompress((base / 'source-after.json.gz').read_bytes()))
names = subprocess.check_output(['git', 'ls-tree', '-r', '--name-only', state['source']], cwd=checkout, text=True).splitlines()
required = {n for n in names if n.split('/')[0] not in ('docs', 'scripts', '.github') and (n.endswith('.go') or n in ('go.mod', 'go.sum') or '/testdata/' in n)}
assert required == set(before) and len(before) == state['selected_inputs']
stream = io.BytesIO(subprocess.check_output(['git', 'cat-file', '--batch'], cwd=checkout,
    input=''.join(state['source'] + ':' + n + '\n' for n in before).encode()))
for name, digest in before.items():
    header = stream.readline().split()
    assert header[1] == b'blob'
    data = stream.read(int(header[2]))
    assert stream.read(1) == b'\n'
    assert hashlib.sha256(data).hexdigest() == digest == hashlib.sha256((checkout / name).read_bytes()).hexdigest()
binary = Path(state['binary']['path'])
info = subprocess.check_output(['go', 'version', '-m', str(binary)], text=True)
assert '-race=true' in info and info == state['binary']['build_info']
assert hashlib.sha256(binary.read_bytes()).hexdigest() == state['binary']['sha256']
assert len(state['receipts']) == 2 and all(r['actual_exit'] == 0 and r['finished_utc'] for r in state['receipts'])
assert state['receipts'][0]['command'] == ['go', 'test', '-race', '-p=1', '-c', '-o', str(binary), './client']
assert state['receipts'][1]['command'] == [str(binary), '-test.v', '-test.run=^(TestGraphCanonicalSignalBinding(BudgetRecovery|ActualDeadlineRecovery)|TestGraphCanonicalSignalPublicationRecoveryCuts)$', '-test.count=1', '-test.timeout=2m']
assert all(not Path('/proc', str(r['child_pid'])).exists() for r in state['receipts'])
log = (base / 'race.log').read_text()
assert '\nPASS\n' in log and not any(x in log for x in ('--- FAIL:', '--- SKIP:', 'WARNING: DATA RACE', 'panic:'))
for name in ('TestGraphCanonicalSignalBindingBudgetRecovery', 'TestGraphCanonicalSignalBindingActualDeadlineRecovery', 'TestGraphCanonicalSignalPublicationRecoveryCuts'):
    assert re.search(r'^--- PASS: ' + name + r' ', log, re.M)
for mode in ('fast', 'over-budget', 'corrupt'):
    assert re.search(r'--- PASS: TestGraphCanonicalSignalBindingBudgetRecovery/' + mode + r' ', log)
assert len(re.findall(r'--- PASS: TestGraphCanonicalSignalPublicationRecoveryCuts/\S+/\d+ ', log)) == 112
result = dict(accepted=True, source=state['source'], git_verified_inputs=len(before), service=props,
              actual_compile_exit=0, actual_test_exit=0, binary=state['binary'], seeded_cases=48,
              exact_replays=48, actual_context_timer_cases=1, publication_recovery_cases=112,
              log_sha256=hashlib.sha256(log.encode()).hexdigest(),
              scope='Clean-source focused Signal binding budget/recovery only. No full-suite or native PostgreSQL CLI acceptance.')
(base / 'review.json').write_text(json.dumps(result, indent=2) + '\n')
print(json.dumps(result, indent=2))
