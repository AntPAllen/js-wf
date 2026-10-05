"""Original Tier2 row selection and retained legacy executable preparation."""
from pathlib import Path
import hashlib
import json
import shutil
import subprocess

ROWS = {
    'journal': 'TestMixedMatrixJournalLeaderEveryThirtySeconds',
    'consumer': 'TestMixedMatrixConsumerLeaderEveryThirtySeconds',
    'cluster': 'TestMixedMatrixAllServersKilledEveryThirtySeconds',
    'partition': 'TestMixedMatrixServerPartitionEveryThirtySeconds',
    'worker': 'TestMixedMatrixRandomWorkerKilledEveryFiveSeconds',
    'pause': 'TestMixedMatrixWorkerPausedFortyFiveSeconds',
    'isolation': 'TestMixedMatrixWorkerReplyIsolationFortyFiveSeconds',
    'workerclock': 'TestMixedMatrixWorkerClockSkew',
    'serverclockplus': 'TestMixedMatrixServerClockSkewPositive',
    'serverclockminus': 'TestMixedMatrixServerClockSkewNegative',
    'fanoutrestart': 'TestMixedMatrixFanoutRestartEveryThirtySeconds',
    'blockdisk': 'TestMixedMatrixBlockDiskStallEveryThirtySeconds',
    'upgrade': 'TestMixedMatrixRollingServerUpgrade',
}
CLOCK_ROWS = frozenset(('workerclock', 'serverclockplus', 'serverclockminus'))


def sdk_timeout(row):
    if row not in ROWS:
        raise ValueError('unknown Tier2 row')
    return '20m' if row in CLOCK_ROWS else '18m'


def legacy_version(build_info):
    for line in build_info.splitlines():
        fields = line.strip().split('\t')
        if len(fields) >= 3 and fields[:2] == ['mod', 'github.com/nats-io/nats-server/v2']:
            return fields[2]
    return None


def capture_legacy_server(source, root):
    if not source.is_absolute() or not source.is_file():
        raise ValueError('upgrade requires an absolute NATS 2.11.17 executable path')
    target = root/'legacy-server.bin'
    shutil.copy2(source, target)
    target.chmod(0o700)
    with target.open('rb') as file:
        digest = hashlib.file_digest(file, 'sha256').hexdigest()
    info = subprocess.check_output(['go', 'version', '-m', str(target)], text=True)
    if legacy_version(info) != 'v2.11.17':
        raise ValueError('retained legacy executable must identify NATS v2.11.17')
    (root/'legacy-server-input.json').write_text(json.dumps({
        'supplied_path': str(source), 'captured': target.name,
        'sha256': digest, 'build_info': info,
        'scope': 'Supplied executable bytes/build information; live process captures remain separate.'
    }, indent=2)+'\n')
    return target
