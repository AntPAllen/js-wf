import hashlib, json, os, pathlib, shutil, subprocess, tarfile, time
root = pathlib.Path('/dev/shm/js-wf-clock-audit-success-37166976657')
repo = pathlib.Path('/home/exedev/js-wf')
restored = root/'restored'
original = root/'original-stores/rolling-originals.tar.gz'
expected = '24005344b8929baca56ef51f88a713aa27ed0d79fbc1e95443d73238fd63bf17'
expected = '24005344b8929baca56ef51f88a713aa27ed0d79fbc1e95443d73238fd63bf17'
# The canonical digest is read from the independently accepted record below;
# never infer it from the current expanded restoration.
record = json.loads((root/'original-archive-verification.json').read_text())
expected = record['archive_sha256']
assert expected == '24005344b8929baca56ef51f88a713aa27ed0d79fbc1e95443d73238fd63bf17'
manifest = json.loads((root/'original-stores/rolling-manifest.json').read_text())['files']
def digest(path):
    h = hashlib.sha256()
    with path.open('rb') as source:
        while data := source.read(1024*1024): h.update(data)
    return h.hexdigest()
assert digest(original) == expected and original.stat().st_size == 108320313
accepted = repo/'docs/scale/worker-clock-checkpoint-2026-10-04/accepted-seed-15'
committed = subprocess.check_output(['git', 'show', '19f3cb36c148b95f94737400dbaa158247814fd6:docs/scale/worker-clock-checkpoint-2026-10-04/accepted-seed-15/proof.tar.gz'], cwd=repo)
accepted_manifest = json.loads((accepted/'manifest.json').read_text())
assert hashlib.sha256(committed).hexdigest() == accepted_manifest['archive_sha256']
assert digest(accepted/'proof.tar.gz') == accepted_manifest['archive_sha256']
seen = set()
with tarfile.open(original, 'r:gz') as archive:
    for member in archive:
        name = pathlib.PurePosixPath(member.name)
        assert member.isfile() and not name.is_absolute() and '..' not in name.parts
        assert member.name in manifest and member.name not in seen
        stream = archive.extractfile(member); h = hashlib.sha256()
        while data := stream.read(1024*1024): h.update(data)
        assert h.hexdigest() == manifest[member.name]
        path = restored/member.name
        assert path.is_file() and not path.is_symlink() and digest(path) == h.hexdigest()
        seen.add(member.name)
assert seen == set(manifest) and len(seen) == 3963
files = []
for path in restored.rglob('*'):
    assert not path.is_symlink()
    if path.is_file():
        assert str(path.relative_to(restored)) in seen
        files.append(path)
assert len(files) == len(seen)
for fd in pathlib.Path('/proc').glob('[0-9]*/fd/*'):
    try: path = os.readlink(fd)
    except OSError: continue
    assert not (path == str(restored) or path.startswith(str(restored)+'/')), (fd, path)
report = dict(verified_at_utc=time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime()),
    accepted_source='c2e8c0122c15ef2d31ffa11c24ccd53c203a233f',
    accepted_proof_commit='19f3cb36c148b95f94737400dbaa158247814fd6',
    canonical_archive=str(original), canonical_sha256=expected,
    canonical_bytes=original.stat().st_size, verified_original_and_restored_members=len(seen),
    committed_compact_proof_sha256_verified=True, removed_duplicate_path=str(restored),
    raw_bytes=sum(p.stat().st_size for p in files),
    allocated_bytes=sum(p.stat().st_blocks*512 for p in files),
    originals_retained=True, failed_originals_untouched=True, qualification_unchanged=True,
    stores_reopened=False, removal_complete=False)
out = root/'restoration-removal.json'
out.write_text(json.dumps(report, indent=2)+'\n')
shutil.rmtree(restored)
report['removal_complete'] = not restored.exists()
assert report['removal_complete'] and original.exists()
out.write_text(json.dumps(report, indent=2)+'\n')
print(out.read_text())
