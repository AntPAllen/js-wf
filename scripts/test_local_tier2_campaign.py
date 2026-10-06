import importlib.util
from pathlib import Path
import tempfile
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
