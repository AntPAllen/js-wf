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


def capture_partition_candidate(source, canonical, root, repo, revision):
    """Retain exact executable bytes tied to a committed component proof."""
    if not source.is_absolute() or source.is_symlink() or not source.is_file():
        raise ValueError('partition candidate requires an absolute regular executable')
    if canonical.is_absolute() or '..' in canonical.parts or not (repo/canonical).resolve().is_relative_to(repo):
        raise ValueError('candidate proof must be a repository-relative directory')
    retained = root/'inputs'/'partition-server-proof'
    retained.mkdir(parents=True)
    records = {}
    for name in ['archive-verification.json', 'fixture-inventory.json', 's3-readback.json',
                 'independent-review.json', 'candidate-build.json', 'candidate-module-inputs.json',
                 'candidate-nats-source-before.json', 'candidate-nats-source-after.json',
                 'candidate-dependencies-before.json', 'candidate-dependencies-after.json',
                 'candidate-overlay.json', 'source-before.json', 'source-after.json']:
        path = repo/canonical/name
        data = subprocess.check_output(['git', 'cat-file', 'blob', revision+':'+str(canonical/name)], cwd=repo)
        if path.is_symlink() or path.read_bytes() != data:
            raise ValueError('candidate proof differs from committed bytes: '+name)
        (retained/name).write_bytes(data)
        records[name] = hashlib.sha256(data).hexdigest()
    read = lambda name: json.loads((retained/name).read_text())
    meta, manifest, receipt, review, build = (read(name) for name in
        ['archive-verification.json', 'fixture-inventory.json', 's3-readback.json', 'independent-review.json', 'candidate-build.json'])
    if (meta['schema'] != 'js-wf-full-fixture-archive-v1'
            or meta.get('all_archive_members_read_back') is not True
            or meta.get('all_current_fixture_files_unchanged_after_capture') is not True
            or hashlib.sha256((retained/'fixture-inventory.json').read_bytes()).hexdigest() != meta['inventory_sha256']
            or receipt['canonical_metadata'] != str(canonical/'archive-verification.json')
            or receipt['archive']['full_readback'] != {'bytes': meta['archive_bytes'], 'sha256': meta['archive_sha256']}
            or review['complete_archive'] != meta or review['source'] != build['source']
            or review['result']['recovered'] is not True or review['configuration_equal_prior_failed_production_component'] is not True
            or review['candidate_sha256'] != build['executable_sha256']
            or review['three_live_candidate_executable_argv_birth_captures_verified'] is not True):
        raise ValueError('candidate requires a verified committed production component recovery and full S3 receipt')
    target = root/'partition-server.bin'
    shutil.copy2(source, target)
    target.chmod(0o700)
    with target.open('rb') as file:
        digest = hashlib.file_digest(file, 'sha256').hexdigest()
    if (digest != build['executable_sha256'] or digest != manifest['files']['candidate-server']['sha256']
            or target.stat().st_size != manifest['files']['candidate-server']['bytes']):
        raise ValueError('supplied partition executable differs from source-bound component candidate')
    info = subprocess.check_output(['go', 'version', '-m', str(target)], text=True)
    (root/'partition-server-input.json').write_text(json.dumps({
        'supplied_path': str(source), 'captured': target.name, 'sha256': digest, 'build_info': info,
        'parent_proof': str(canonical), 'parent_proof_revision': revision, 'parent_source': build['source'],
        'retained_parent_records': records, 'parent_archive_url': receipt['archive']['url'],
        'parent_archive_sha256': meta['archive_sha256'],
        'scope': 'Experimental exact source-bound component executable; candidate native row only. Default NATS and original campaign verdict unchanged.'
    }, indent=2)+'\n')
    return target
