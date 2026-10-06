import importlib.util
from pathlib import Path
import tempfile
import io
import tarfile
import unittest

spec = importlib.util.spec_from_file_location('originals',Path(__file__).with_name('verify-tier2-closed-originals.py'))
r = importlib.util.module_from_spec(spec); spec.loader.exec_module(r)

class ClosedOriginals(unittest.TestCase):
    def test_manifest_must_match_single_archived_member(self):
        for mutation in ('none','changed','missing','duplicate'):
            with self.subTest(mutation=mutation), tempfile.TemporaryDirectory() as tmp:
                donor=Path(tmp); (donor/'archive-manifest.json').write_bytes(b'{}')
                with tarfile.open(donor/'proof.tar.gz','w:gz') as archive:
                    for _ in range(0 if mutation == 'missing' else 2 if mutation == 'duplicate' else 1):
                        member=tarfile.TarInfo('archive-manifest.json'); member.size=2
                        archive.addfile(member,io.BytesIO(b'{}'))
                if mutation == 'changed': (donor/'archive-manifest.json').write_bytes(b'{"forged": true}')
                if mutation == 'none': self.assertEqual(r.load_preserved_manifest(donor),{})
                else:
                    with self.assertRaises(ValueError): r.load_preserved_manifest(donor)

    def test_exact_files_and_fail_closed_corruption(self):
        for mutation in ('none','changed','missing','extra','symlink','empty_manifest'):
            with self.subTest(mutation=mutation), tempfile.TemporaryDirectory() as tmp:
                donor=Path(tmp); source=donor/'originals'/'TestMixedMatrixFixture'; source.mkdir(parents=True)
                path=source/'data'; path.write_bytes(b'original')
                manifest={'originals/TestMixedMatrixFixture/data':dict(bytes=8,sha256=r.sha(path))}
                if mutation == 'changed': path.write_bytes(b'mutated!')
                elif mutation == 'missing': path.unlink()
                elif mutation == 'extra': (source/'extra').write_bytes(b'new')
                elif mutation == 'symlink': (source/'alias').symlink_to(path)
                elif mutation == 'empty_manifest': manifest={}
                if mutation == 'none': self.assertEqual(r.verify_original_files(source,donor,manifest),1)
                else:
                    with self.assertRaises(ValueError): r.verify_original_files(source,donor,manifest)

    def test_open_original_descriptor_is_rejected(self):
        with tempfile.TemporaryDirectory() as tmp:
            source=Path(tmp); path=source/'open'; path.write_bytes(b'original')
            with path.open('rb'):
                with self.assertRaises(ValueError): r.verify_no_open_originals(source)
            self.assertGreater(r.verify_no_open_originals(source)['descriptors_observed'],0)

if __name__ == '__main__': unittest.main()
