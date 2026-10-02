import gzip
import hashlib
import importlib.util
import json
from pathlib import Path
import tempfile
import unittest

spec=importlib.util.spec_from_file_location('restore',Path(__file__).with_name('restore-fixture-archive.py'))
module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module)

class RestoreFixtureArchiveTests(unittest.TestCase):
 def fixture(self,root):
  raw=b'\x00binary\xff fixture'*1000
  compressed=root/'archive'/'file.gz';compressed.parent.mkdir();compressed.write_bytes(gzip.compress(raw,mtime=0))
  entry=dict(original='node-0/jetstream/$G/messages.blk',archive='archive/file.gz',bytes=len(raw),sha256=hashlib.sha256(raw).hexdigest(),mode=0o600,mtime_ns=1700000000000000000)
  manifest=root/'manifest.jsonl';manifest.write_text(json.dumps(entry)+'\n')
  return raw,entry,manifest
 def test_restore_bytes_metadata_and_no_overwrite(self):
  with tempfile.TemporaryDirectory() as folder:
   root=Path(folder);raw,entry,manifest=self.fixture(root);dest=root/'restored'
   result=module.restore(root,manifest,dest);p=dest/entry['original']
   self.assertEqual(result['files'],1);self.assertEqual(p.read_bytes(),raw)
   self.assertEqual(p.stat().st_mtime_ns,entry['mtime_ns']);self.assertEqual(p.stat().st_mode&0o777,0o600)
   with self.assertRaises(FileExistsError):module.restore(root,manifest,dest)
 def test_corrupt_bytes_or_escape_rejected(self):
  for change in ({'sha256':'0'*64},{'bytes':1},{'original':'../escaped'},{'archive':'/etc/passwd'}):
   with self.subTest(change=change),tempfile.TemporaryDirectory() as folder:
    root=Path(folder);raw,entry,manifest=self.fixture(root);entry.update(change);manifest.write_text(json.dumps(entry)+'\n')
    with self.assertRaises(ValueError):module.restore(root,manifest,root/'restored')

if __name__=='__main__':unittest.main()
