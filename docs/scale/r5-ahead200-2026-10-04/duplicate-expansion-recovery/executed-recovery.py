from pathlib import Path, PurePosixPath
import datetime, hashlib, json, os, shutil, subprocess, tarfile, zipfile

repo = Path('/home/exedev/js-wf')
root = Path('/tmp/js-wf-ahead200-raw-review-37117571555-20261004')
doc = repo / 'docs/scale/r5-ahead200-2026-10-04/complete-recorded-source-raw'
out = repo / 'docs/scale/r5-ahead200-2026-10-04/duplicate-expansion-recovery'
head = subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd=repo, text=True).strip()
assert not subprocess.check_output(['git', 'status', '--porcelain'], cwd=repo)
assert subprocess.check_output(['git', 'ls-remote', 'origin', 'refs/heads/main'], cwd=repo, text=True).split()[0] == head

def sha(p):
    with p.open('rb') as f:
        return hashlib.file_digest(f, 'sha256').hexdigest()

m = json.loads((doc / 'manifest.json').read_text())
a = json.loads((doc / 'aggregate.json').read_text())
assert a['accepted_seeds'] == 200 and a['full_recorded_source_ahead_clock_row_qualified']
assert m['all_members_readback_verified'] and m['all_parts_readback_verified']
for name in ['manifest.json', 'aggregate.json']:
    assert hashlib.sha256(subprocess.check_output(['git', 'show', head + ':' + str((doc/name).relative_to(repo))], cwd=repo)).hexdigest() == sha(doc/name)
archive = root / 'proof.tar.gz'
assert sha(archive) == m['archive_sha256'] and archive.stat().st_size == m['archive_bytes']
combined = hashlib.sha256()
total = 0
for part in m['parts']:
    p = doc / part['path']
    assert not p.is_symlink() and sha(p) == part['sha256'] and p.stat().st_size == part['bytes']
    proc = subprocess.Popen(['git', 'show', head + ':' + str(p.relative_to(repo))], cwd=repo, stdout=subprocess.PIPE)
    h = hashlib.sha256()
    size = 0
    for b in iter(lambda: proc.stdout.read(1024 * 1024), b''):
        combined.update(b); h.update(b); size += len(b)
    assert proc.wait() == 0 and h.hexdigest() == part['sha256'] and size == part['bytes']
    total += size
assert combined.hexdigest() == m['archive_sha256'] and total == m['archive_bytes']
seen = set()
with tarfile.open(archive, 'r:gz') as t:
    for member in t:
        name = PurePosixPath(member.name)
        assert member.isfile() and not name.is_absolute() and '..' not in name.parts and name.as_posix() == member.name
        assert member.name not in seen and member.name in m['files']
        e = m['files'][member.name]
        assert member.size == e['bytes'] and hashlib.file_digest(t.extractfile(member), 'sha256').hexdigest() == e['sha256']
        seen.add(member.name)
assert seen == set(m['files'])
out.mkdir(exist_ok=False)
shutil.copyfile(__file__, out / 'executed-recovery.py')
reports = []
for seed in range(1, 201):
    folder = root / f'seed-{seed}'
    target = folder / 'raw'
    r = json.loads((folder / 'row-review.json').read_text())
    assert r['shard_qualified'] and r['first_seed'] == r['last_seed'] == seed and r['source'] == a['source']
    prefix = f'seed-{seed}/raw/'
    expected = {n[len(prefix):]: e['sha256'] for n, e in m['files'].items() if n.startswith(prefix)}
    assert expected == r['artifact_sha256'] and target.is_dir() and not target.is_symlink()
    paths = list(target.rglob('*'))
    assert not any(p.is_symlink() for p in paths)
    actual = {str(p.relative_to(target)): p for p in paths if p.is_file()}
    assert set(actual) == set(expected)
    for name, p in actual.items():
        assert sha(p) == expected[name]
    zip_path = folder / 'raw.zip'
    assert sha(zip_path) == m['files'][f'seed-{seed}/raw.zip']['sha256']
    with zipfile.ZipFile(zip_path) as z:
        assert len(z.namelist()) == len(expected) and set(z.namelist()) == set(expected)
        for name, digest in expected.items():
            with z.open(name) as f:
                assert hashlib.file_digest(f, 'sha256').hexdigest() == digest
    opened = []
    for fd in Path('/proc').glob('[0-9]*/fd/*'):
        try:
            link = os.readlink(fd)
        except (FileNotFoundError, PermissionError):
            continue
        if link == str(target) or link.startswith(str(target) + '/'):
            opened.append(str(fd))
    assert not opened, opened
    inodes = {}
    for p in actual.values():
        s = p.stat()
        v = inodes.setdefault((s.st_dev, s.st_ino), dict(paths=0, links=s.st_nlink, allocated=s.st_blocks*512))
        v['paths'] += 1
    recovered = sum(v['allocated'] for v in inodes.values() if v['paths'] == v['links'])
    report = dict(seed=seed, target=str(target), files=len(actual), zip_sha256=sha(zip_path), expanded_and_zip_hashes_verified=True, visible_open_fds=opened, exclusive_allocated_bytes_recovered=recovered, removed=False)
    reports.append(report)
    def save():
        (out/'recovery.json').write_text(json.dumps(dict(observed_at=datetime.datetime.now(datetime.timezone.utc).isoformat(), proof_commit=head, pushed_main_verified=True, canonical_archive=str(archive), canonical_archive_sha256=m['archive_sha256'], all_canonical_members_and_committed_parts_verified=True, reports=reports, recovered_bytes=sum(x['exclusive_allocated_bytes_recovered'] for x in reports if x['removed']), raw_zips_and_models_retained=True, failed_originals_unchanged=True), indent=2)+'\n')
    save()
    shutil.rmtree(target)
    assert not target.exists()
    report['removed'] = True
    save()
    if seed % 20 == 0:
        print('Recovered', seed, 'of 200 expansions', flush=True)
print('COMPLETE', sum(x['exclusive_allocated_bytes_recovered'] for x in reports), flush=True)
