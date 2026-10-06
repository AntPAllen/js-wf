#!/usr/bin/env python3
"""Audit a fresh copy of one canonically preserved full fanout boundary case."""
import argparse
from datetime import datetime, timezone
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import shutil
import subprocess

REPO = Path(__file__).resolve().parent.parent
SCRIPTS = REPO / 'scripts'
TEST = 'TestFiveHundredChildFanoutCombinedParentAndJournalBoundaryMatrix'


def module(name, filename):
    spec = importlib.util.spec_from_file_location(name, SCRIPTS / filename)
    result = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(result)
    return result


closed = module('fanout_closed', 'verify-tier2-closed-originals.py')
combined = module('fanout_combined', 'review-fanout-combined.py')
fixture = module('fanout_fixture', 'fixture_delta.py')
physical = combined.physical
require, sha, read = closed.require, closed.sha, physical.read


def git(*args):
    return subprocess.check_output(['git', *args], cwd=REPO)


def write(path, value):
    path.write_text(json.dumps(value, indent=2) + '\n')


def inventory(root):
    result = {}
    for path in sorted(root.rglob('*')):
        require(not path.is_symlink(), 'symlink in copied store scope')
        if path.is_file():
            result[path.relative_to(root).as_posix()] = dict(bytes=path.stat().st_size, sha256=sha(path))
    require(result, 'empty store inventory')
    return result


def verify_donor(donor, canonical, phase, position, revision):
    require(donor.is_absolute() and donor.is_dir() and not donor.is_relative_to(REPO), 'invalid closed donor')
    require(not canonical.is_absolute() and '..' not in canonical.parts, 'unsafe canonical directory')
    # Read all committed parts/members, including their embedded manifest. No
    # NATS process is started against the donor, and no raw duplicate is needed.
    manifest = fixture.read_base(REPO, revision, (canonical / 'archive-verification.json').as_posix())
    require(read(donor / 'archive-manifest.json') == manifest, 'donor manifest differs from canonical archive')
    for name, expected in manifest.items():
        path = donor / name
        require(path.is_file() and not path.is_symlink() and
                dict(bytes=path.stat().st_size, sha256=sha(path)) == expected,
                'current donor member differs from canonical archive: ' + name)
    native = combined.review(donor)
    require(native == read(donor / 'independent-review.json'), 'current native review differs from preserved review')
    source = donor / 'originals' / TEST / phase / position / 'cluster'
    closed.verify_original_files(source, donor, manifest)
    descriptors = closed.verify_no_open_originals(source)
    require(git('rev-parse', 'HEAD').decode().strip() == revision, 'HEAD changed during donor verification')
    return source, dict(canonical_revision=revision, canonical_proof=canonical.as_posix(),
                        canonical_all_parts_members_manifest_verified=True,
                        all_current_donor_members_match=True, native_review=native,
                        selected_phase=phase, selected_position=position, **descriptors)


def review(root):
    before = read(root / 'copy-before.json')
    source = Path(before['original_root'])
    canonical = read(root / 'canonical-verification.json')
    inputs = read(root / 'selected-inputs.json')
    require(canonical['canonical_revision'] == inputs['helper_git_source'], 'canonical proof and helper source differ')
    manifest = fixture.read_base(REPO, canonical['canonical_revision'],
                                 canonical['canonical_proof'] + '/archive-verification.json')
    donor = source.parents[4]
    require(source == donor / 'originals' / TEST / canonical['selected_phase'] / canonical['selected_position'] / 'cluster',
            'original case selection differs from canonical verification')
    for name in ('independent-review.json', 'source-before.json', 'external-source-before.json'):
        path = donor / name
        require(dict(bytes=path.stat().st_size, sha256=sha(path)) == manifest[name], 'native binding differs from committed archive')
    require(canonical['native_review'] == read(donor / 'independent-review.json'), 'native review differs from committed record')
    closed.verify_original_files(source, donor, manifest)
    require(inventory(source) == before['files'] == read(root / 'original-after.json')['files'], 'original bytes changed')
    require(inventory(root / 'cluster') == read(root / 'copy-after.json')['files'], 'closed copy changed since audit')
    closed.verify_no_open_originals(root / 'cluster')
    e = read(root / 'execution.json'); binary = read(root / 'binary.json')
    require(e['status'] == 'passed' and e['exit_code'] == 0 and not Path('/proc', str(e['pid'])).exists(), 'copied SDK failed or live')
    require(sha(root / 'review-sdk') == e['actual_sha256'] == binary['sha256'] and
            e['actual_build_info'].splitlines()[1:] == binary['build_info'].splitlines()[1:], 'copied SDK identity differs')
    native_source = read(donor / 'source-before.json')
    native_external = read(donor / 'external-source-before.json')
    require(inputs['production_source'] == native_source['revision'] == canonical['native_review']['source'], 'production source differs')
    for name, item in inputs['files'].items():
        require(sha(root / item['captured']) == item['sha256'], 'captured build input differs: ' + name)
        path = Path(name)
        if path.is_relative_to(REPO):
            relative = path.relative_to(REPO).as_posix()
            require(item['sha256'] == native_source['files'][relative] ==
                    hashlib.sha256(git('show', inputs['production_source'] + ':' + relative)).hexdigest(), 'production Git binding differs')
        else:
            require(item['sha256'] == native_external[name], 'external native binding differs')
    for name, digest in inputs['module_files'].items():
        require(sha(root / 'selected-source' / name) == digest ==
                hashlib.sha256(git('show', inputs['production_source'] + ':' + name)).hexdigest(), 'module binding differs')
    for filename in ('fanout-retained-review.go.txt', 'run-fanout-copied-audit.py'):
        captured = root / ('helper.go' if filename.endswith('.txt') else 'executed-producer.py')
        require(captured.read_bytes() == git('show', inputs['helper_git_source'] + ':scripts/' + filename), 'helper/producer Git binding differs')
    require(canonical['canonical_all_parts_members_manifest_verified'] and canonical['all_current_donor_members_match'], 'canonical verification missing')
    case = next(item for item in canonical['native_review']['cases']
                if item['phase'] == canonical['selected_phase'] and item['position'] == canonical['selected_position'])
    report = read(root / 'retained-review.json')
    require(report['report'] == case['retained_report'] and 0 < report['audit_ns'] <= report['whole_review_ns'] < 20_000_000_000,
            'copied complete cohort or original audit budget differs')
    require(report['physically_drained'] is True and report['observed_partition_consumers'] == 64 and
            len(report['child_ids']) == len(set(report['child_ids'])) == 500, 'copied results or durable census invalid')
    physical.review_physical_peers(report, True)
    for peer in report['queue_all_three_peers']:
        require(peer['state']['messages'] == 0 and peer['state']['consumer_count'] == 64 and
                peer['config']['num_replicas'] == 3 and len(peer['cluster']['replicas']) == 2 and
                all(p['current'] and not p.get('offline', False) for p in peer['cluster']['replicas']), 'copied API queue/replicas invalid')
    require(len(report['queue_all_three_peers']) == 3, 'incomplete API views')
    return dict(phase=case['phase'], position=case['position'], original_source=inputs['production_source'],
                helper_source=inputs['helper_git_source'], actual_sdk_pid=e['pid'], actual_sdk_sha256=e['actual_sha256'],
                retained_report=report['report'], audit_ns=report['audit_ns'], whole_review_ns=report['whole_review_ns'],
                selected_inputs=len(inputs['files']), original_files=len(before['files']),
                complete_results_and_three_local_physical_queues_verified=True,
                scope='Fresh copied case only; native full-six verdict, other copied cases, matrices and soak remain separate')


def run(args):
    donor, root = args.donor.resolve(), args.root.resolve()
    require(args.donor.is_absolute() and args.root.is_absolute() and not root.exists() and
            not root.is_relative_to(REPO) and not root.is_relative_to(donor), 'require fresh absolute copy root outside donor and checkout')
    require(not git('status', '--porcelain'), 'require clean checkout')
    head = git('rev-parse', 'HEAD').decode().strip()
    require(git('ls-remote', 'origin', 'refs/heads/main').decode().split()[0] == head, 'HEAD must be pushed to main')
    require(git('branch', '--show-current').decode().strip() == 'main', 'require main branch')
    source, verification = verify_donor(donor, args.canonical_proof, args.phase, args.position, head)
    original = inventory(source)
    root.mkdir(parents=True)
    write(root / 'canonical-verification.json', verification)
    shutil.copy2(__file__, root / 'executed-producer.py')
    shutil.copytree(source, root / 'cluster')
    require(inventory(root / 'cluster') == original, 'initial copy differs')
    write(root / 'copy-before.json', dict(original_root=str(source), files=original, all_initial_copy_bytes_match=True))
    helper = root / 'helper.go'
    helper.write_bytes(git('show', head + ':scripts/fanout-retained-review.go.txt'))
    bound = read(donor / 'source-before.json'); external = read(donor / 'external-source-before.json')
    deps = subprocess.check_output(['go', 'list', '-deps', '-f', '{{.Dir}}|{{join .GoFiles " "}}|{{join .CgoFiles " "}}', str(helper)], cwd=REPO, text=True)
    (root / 'dependencies.txt').write_text(deps)
    inputs = {}
    for line in deps.splitlines():
        directory, *groups = line.split('|')
        for name in ' '.join(groups).split():
            path = (Path(directory) / name).resolve()
            if path == helper:
                continue
            digest = sha(path)
            if path.is_relative_to(REPO):
                name = path.relative_to(REPO).as_posix()
                require(bound['files'][name] == digest == sha(donor / 'source' / name) ==
                        hashlib.sha256(git('show', bound['revision'] + ':' + name)).hexdigest(), 'production input differs from native Git')
                dest = root / 'selected-source' / name
            else:
                require(external[str(path)] == digest, 'external input differs from native build')
                dest = root / 'selected-external-source' / str(path).lstrip('/')
            dest.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(path, dest)
            inputs[str(path)] = dict(sha256=digest, captured=dest.relative_to(root).as_posix())
    for name in ('go.mod', 'go.sum'):
        require(sha(REPO / name) == bound['files'][name], 'native module input changed')
        shutil.copy2(REPO / name, root / 'selected-source' / name)
    write(root / 'selected-inputs.json', dict(production_source=bound['revision'], helper_git_source=head,
          files=inputs, module_files={name: bound['files'][name] for name in ('go.mod', 'go.sum')}))
    env = {key: value for key, value in os.environ.items() if not key.startswith('WF_')}
    env.update(GOCACHE=args.go_cache, GOMAXPROCS='2', GOMEMLIMIT='2GiB')
    build = ['go', 'build', '-p=1', '-buildvcs=true', '-o', str(root / 'review-sdk'), str(helper)]
    command = [str(root / 'review-sdk'), str(root / 'cluster'), '501', str(root / 'retained-review.json')]
    write(root / 'commands.json', dict(build=build, run=command, environment={key: env[key] for key in ('GOCACHE', 'GOMAXPROCS', 'GOMEMLIMIT')}))
    with (root / 'build.log').open('w') as output:
        subprocess.run(build, cwd=REPO, env=env, stdout=output, stderr=subprocess.STDOUT, check=True)
    binary = dict(sha256=sha(root / 'review-sdk'), build_info=subprocess.check_output(['go', 'version', '-m', str(root / 'review-sdk')], text=True))
    write(root / 'binary.json', binary)
    with (root / 'native.log').open('w') as output:
        child = subprocess.Popen(command, cwd=REPO, env=env, stdout=output, stderr=subprocess.STDOUT)
        try:
            live = Path('/proc', str(child.pid), 'exe')
            e = dict(pid=child.pid, actual_sha256=sha(live), actual_build_info=subprocess.check_output(['go', 'version', '-m', str(live)], text=True),
                     status='running', started_utc=datetime.now(timezone.utc).isoformat(), native_R3_servers_embedded_in_sdk=True)
            require(e['actual_sha256'] == binary['sha256'], 'actual SDK bytes differ')
            write(root / 'execution.json', e)
            print('COPY_REVIEW_STARTED', child.pid, flush=True)
            code = child.wait()
        finally:
            if child.poll() is None:
                child.terminate(); child.wait(timeout=30)
    e.update(status='passed' if code == 0 else 'failed', exit_code=code, finished_utc=datetime.now(timezone.utc).isoformat())
    write(root / 'execution.json', e)
    write(root / 'original-after.json', dict(files=inventory(source)))
    write(root / 'copy-after.json', dict(files=inventory(root / 'cluster')))
    require(inventory(source) == original, 'originals changed during copied audit')
    for path, item in inputs.items():
        require(sha(Path(path)) == sha(root / item['captured']) == item['sha256'], 'build inputs changed during copied audit')
    require(code == 0, 'copied helper failed; inspect retained log')
    result = review(root)
    write(root / 'independent-review.json', result)
    print(json.dumps(result, indent=2), flush=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root', type=Path, required=True)
    parser.add_argument('--review-only', action='store_true')
    parser.add_argument('--donor', type=Path)
    parser.add_argument('--canonical-proof', type=Path)
    parser.add_argument('--phase', choices=('create', 'results'))
    parser.add_argument('--position', choices=('first', 'interior', 'last'))
    parser.add_argument('--go-cache', default='/tmp/js-wf-go-build-cache-20261004')
    args = parser.parse_args()
    if args.review_only:
        print(json.dumps(review(args.root.resolve()), indent=2))
    else:
        require(all((args.donor, args.canonical_proof, args.phase, args.position)), 'donor/canonical proof/phase/position required')
        run(args)


if __name__ == '__main__':
    main()
