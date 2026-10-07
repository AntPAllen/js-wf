#!/usr/bin/env python3
"""Verify preserved closed native Tier2 bytes before copying; never open NATS."""
import argparse
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import subprocess
import tarfile

REPO = Path(__file__).resolve().parent.parent


def require(value, message):
    if not value:
        raise ValueError(message)


def sha(path):
    with path.open('rb') as file:
        return hashlib.file_digest(file, 'sha256').hexdigest()


def load_preserved_manifest(donor):
    current = (donor/'archive-manifest.json').read_bytes()
    found = 0
    with tarfile.open(donor/'proof.tar.gz', 'r|gz') as archive:
        for member in archive:
            if member.name == 'archive-manifest.json':
                require(member.isfile(), 'archived manifest is not a file')
                require(archive.extractfile(member).read() == current, 'current manifest differs from preserved archive')
                found += 1
    require(found == 1, 'missing or duplicate preserved manifest')
    return json.loads(current)


def verify_original_files(source, donor, manifest):
    require(source.is_dir() and source.is_relative_to(donor/'originals'), 'invalid original store scope')
    prefix = str(source.relative_to(donor))+'/'
    expected = {name[len(prefix):]:value for name,value in manifest.items() if name.startswith(prefix)}
    require(expected, 'preserved manifest contains no original files')
    actual = {}
    for path in source.rglob('*'):
        require(not path.is_symlink(), 'symlink media requires separate verified copied-image procedure')
        if path.is_file():
            actual[str(path.relative_to(source))] = dict(bytes=path.stat().st_size, sha256=sha(path))
    require(actual == expected, 'original file set or bytes differ from preserved manifest')
    return len(actual)


def verify_no_open_originals(source):
    checked = 0
    unobservable = []
    for process in Path('/proc').glob('[0-9]*'):
        try:
            tasks = list((process/'task').iterdir())
        except (FileNotFoundError, ProcessLookupError):
            continue
        except PermissionError:
            unobservable.append(str(process/'task'))
            continue
        # Report permission limits explicitly. Only visible descriptors can be
        # checked; this does not establish global process lifetime coverage.
        for task in tasks:
            try:
                descriptors = list((task/'fd').iterdir())
            except (FileNotFoundError, ProcessLookupError):
                continue
            except PermissionError:
                unobservable.append(str(task/'fd'))
                continue
            for fd in descriptors:
                try:
                    target = os.readlink(fd)
                except (FileNotFoundError, ProcessLookupError):
                    continue
                except PermissionError:
                    unobservable.append(str(fd))
                    continue
                checked += 1
                if target.endswith(' (deleted)'):
                    target = target[:-10]
                require(not Path(target).is_absolute() or
                        not Path(target).is_relative_to(source), 'original store descriptor remains open: '+str(fd))
    return dict(descriptors_observed=checked, unobservable_paths=unobservable)


def verify(donor, canonical):
    require(donor.is_absolute() and donor.is_dir() and not donor.is_relative_to(REPO), 'require absolute donor outside checkout')
    require(not canonical.is_absolute() and '..' not in canonical.parts, 'canonical proof must be a repository relative directory')
    git = lambda *args: subprocess.check_output(['git', *args], cwd=REPO)
    head = git('rev-parse', 'HEAD').decode().strip()
    require(git('branch', '--show-current').decode().strip() == 'main', 'require main')
    remote = git('ls-remote', 'origin', 'refs/heads/main').decode().split()[0]
    require(head == remote, 'current main must already be pushed')
    blob = lambda name: git('cat-file', 'blob', head+':'+str(canonical/name))
    meta = json.loads(blob('archive-verification.json'))
    require(meta['all_archive_members_and_parts_read_back'] and meta['parts'], 'proof has no complete archive readback')
    combined = hashlib.sha256(); size = 0; seen = set()
    for part in meta['parts']:
        name = part['file']
        require(Path(name).name == name and name not in seen, 'unsafe or duplicate proof part')
        seen.add(name); data = blob(name)
        require(len(data) == part['bytes'] and hashlib.sha256(data).hexdigest() == part['sha256'], 'canonical proof part differs from reviewed metadata')
        combined.update(data); size += len(data)
    require(size == meta['archive_bytes'] and combined.hexdigest() == meta['archive_sha256'], 'canonical full archive differs from review')
    require(sha(donor/'proof.tar.gz') == meta['archive_sha256'], 'current native archive differs from pushed proof')
    manifest = load_preserved_manifest(donor)
    for name in ('execution.json', 'acceptance.json', 'integration.test',
                 'observed-servers.json', 'observed-workers.json'):
        require(manifest[name] == dict(bytes=(donor/name).stat().st_size, sha256=sha(donor/name)),
                'current native identity or executable differs from preserved proof: '+name)
    execution = json.loads((donor/'execution.json').read_text())
    require(execution['status'] == 'passed' and execution['exit_code'] == 0 and
            execution['server_observer_errors'] == 0, 'require successful observed original producer')
    require(json.loads((donor/'acceptance.json').read_text())['exit_code'] == 0, 'native duration acceptance missing')
    require(sha(donor/'integration.test') == execution['sha256'], 'original SDK executable changed')
    processes = [execution['pid']]
    servers = json.loads((donor/'observed-servers.json').read_text())
    require({s['node'] for s in servers} == {0,1,2}, 'three actual native servers not observed')
    for name in ('observed-servers.json', 'observed-workers.json'):
        for record in json.loads((donor/name).read_text()):
            processes.append(record['pid'])
            require(sha(donor/record['captured']) == record['actual_executable_sha256'], 'captured executable bytes changed')
    require(all(not Path('/proc',str(pid)).exists() for pid in processes), 'observed SDK, worker or server remains live')
    test = execution['test']
    require(test.startswith('TestMixedMatrix') and Path(test).name == test and test not in ('.','..'), 'unsafe original test scope')
    source = donor/'originals'/test
    count = verify_original_files(source, donor, manifest)
    descriptors = verify_no_open_originals(source)
    require(git('rev-parse','HEAD').decode().strip() == head, 'main changed during verification')
    return dict(observed_utc=datetime.now(timezone.utc).isoformat(), head=head,
                canonical_proof=str(canonical), canonical_archive_sha256=meta['archive_sha256'],
                archive_manifest_sha256=sha(donor/'archive-manifest.json'),
                canonical_parts_and_current_archive_match=True,
                all_current_original_store_files_match_preserved_manifest=True, files=count,
                sdk_workers_observed_servers_closed=True, observed_processes=len(processes),
                all_visible_task_fds_checked=True, **descriptors,
                scope='Closed native bytes verified before fresh copy; original fault qualification and copied audit remain separate')


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--donor', type=Path, required=True)
    parser.add_argument('--canonical-proof', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    donor = args.donor.resolve()
    require(args.donor.is_absolute() and not args.output.resolve().is_relative_to(donor/'originals'),
            'require absolute donor and output outside original stores')
    result = verify(donor,args.canonical_proof)
    args.output.write_text(json.dumps(result,indent=2)+'\n')
    print(json.dumps({key:value for key,value in result.items() if key != 'unobservable_paths'},indent=2))
    print('unobservable_paths='+str(len(result['unobservable_paths'])))

if __name__ == '__main__': main()
