import importlib.util
from pathlib import Path
import tempfile
import json
import subprocess
import sys
import hashlib
import unittest

from retained_input_cache import retain

spec = importlib.util.spec_from_file_location('local_campaign', Path(__file__).with_name('run-local-tier2-partition-campaign.py'))
campaign = importlib.util.module_from_spec(spec)
spec.loader.exec_module(campaign)


class LocalCampaignControls(unittest.TestCase):
    def test_full_original_coverage_and_rejections(self):
        full = [dict(seed=n, exit_code=0) for n in range(1, 201)]
        self.assertTrue(campaign.coverage(full))
        self.assertFalse(campaign.coverage(full[:199]))
        self.assertFalse(campaign.coverage([dict(seed=n, exit_code=(1 if n == 37 else 0)) for n in range(1, 201)]))
        for invalid in (full[1:], full+[full[-1]], [full[1], full[0]]+full[2:]):
            with self.assertRaises(ValueError):
                campaign.coverage(invalid)

    def test_candidate_profile_requires_exact_closed_evidence(self):
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory)
            (root/'partition-server.bin').write_bytes(b'exact component candidate')
            digest=hashlib.sha256((root/'partition-server.bin').read_bytes()).hexdigest()
            records={'acceptance.json':dict(server_profile='experimental-component-candidate'),
                     'partition-server-input.json':dict(sha256=digest),
                     'partition-server-verification.json':dict(passed=True,expected_sha256=digest,observed_peers=3)}
            def save():
                for name,value in records.items():(root/name).write_text(json.dumps(value))
            save()
            self.assertEqual(campaign.verify_seed_profile(root,'experimental-component-candidate',digest),digest)
            for name,field,value in [('acceptance.json','server_profile','default'),
                                     ('partition-server-verification.json','passed',1),
                                     ('partition-server-verification.json','observed_peers',2),
                                     ('partition-server-verification.json','expected_sha256','0'*64),
                                     ('partition-server-input.json','sha256','0'*64)]:
                prior=records[name][field];records[name][field]=value;save()
                with self.assertRaises(ValueError):campaign.verify_seed_profile(root,'experimental-component-candidate',digest)
                records[name][field]=prior;save()
            (root/'partition-server.bin').write_bytes(b'changed candidate')
            with self.assertRaises(ValueError):campaign.verify_seed_profile(root,'experimental-component-candidate',digest)
            (root/'acceptance.json').write_text(json.dumps(dict(server_profile='default')))
            self.assertIsNone(campaign.verify_seed_profile(root,'default'))
            rejected=[dict(seed=n,exit_code=0,profile_verified=(n!=37)) for n in range(1,201)]
            self.assertFalse(campaign.coverage(rejected))

    def test_partial_candidate_selection_rejects_before_root_creation(self):
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory)/'not-created'
            script=Path(__file__).with_name('run-local-tier2-partition-campaign.py')
            for options in [['--partition-server','/not/present'],
                            ['--partition-server-proof','docs/proof'],
                            ['--partition-server','relative','--partition-server-proof','docs/proof']]:
                result=subprocess.run([sys.executable,str(script),'--root',str(root),*options],capture_output=True)
                self.assertEqual(result.returncode,2)
                self.assertFalse(root.exists())

    def test_shared_capture_preserves_bytes_and_rejects_corruption(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source = root/'source'
            source.write_bytes(b'selected immutable input')
            for name in ('first', 'second'):
                retain(source, root/name, root/'cache')
            self.assertEqual((root/'first').stat().st_ino, (root/'second').stat().st_ino)
            self.assertEqual(source.read_bytes(), (root/'first').read_bytes())
            self.assertEqual(source.stat().st_mtime_ns, (root/'first').stat().st_mtime_ns)
            (root/'first').write_bytes(b'corrupted captured bytes')
            with self.assertRaises(ValueError):
                retain(source, root/'third', root/'cache')
            self.assertFalse((root/'third').exists())
            source_link = root/'source-link'
            source_link.symlink_to(source)
            with self.assertRaises(ValueError):
                retain(source_link, root/'fourth', root/'cache')


if __name__ == '__main__':
    unittest.main()
