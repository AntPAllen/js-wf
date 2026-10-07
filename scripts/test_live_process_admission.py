import hashlib
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

import live_process_admission as admission


class LiveProcessAdmission(unittest.TestCase):
    def exercise(self, mutate=None, wrong=None):
        profile = {'WF_ADMISSION_CONTROL':'actual'}
        command = [sys.executable, '-c', 'import time; time.sleep(.4)']
        digest = hashlib.sha256(Path(sys.executable).read_bytes()).hexdigest()
        with tempfile.TemporaryDirectory() as cwd:
            child = subprocess.Popen(command, cwd=cwd, env=dict(os.environ, **profile))
            try:
                original = admission.snapshot
                count = 0
                def observed(pid, expected):
                    nonlocal count
                    row = original(pid, expected)
                    count += 1
                    if mutate and count == 1:
                        mutate(row)
                    return row
                args = dict(command=command, profile=profile, cwd=cwd, binary=sys.executable, expected_sha256=digest)
                if wrong:
                    args.update(wrong(cwd))
                with patch.object(admission, 'snapshot', observed):
                    return admission.admit(child, **args)
            finally:
                if child.poll() is None:
                    child.terminate()
                child.wait()

    def test_incomplete_exec_environment_is_reobserved(self):
        row = self.exercise(mutate=lambda r:r['environment'].clear())
        self.assertEqual(row['environment'], {'WF_ADMISSION_CONTROL':'actual'})
        self.assertEqual(row['admission']['first_missing_environment_keys'], ['WF_ADMISSION_CONTROL'])
        self.assertGreater(row['admission']['observations'], 1)
        self.assertTrue(row['admission']['stable_identity_observed_twice'])

    def test_real_identity_admitted(self):
        self.assertTrue(self.exercise()['admission']['stable_identity_observed_twice'])

    def test_wrong_identity_never_substituted_from_intent(self):
        for change in (
            lambda cwd:{'profile':{'WF_ADMISSION_CONTROL':'different'}},
            lambda cwd:{'command':[sys.executable,'-c','different']},
            lambda cwd:{'cwd':'/'},
            lambda cwd:{'expected_sha256':'0'*64},
            lambda cwd:{'binary':'/bin/false'},
        ):
            with self.subTest(change=change), self.assertRaisesRegex(RuntimeError, 'exited before stable identity'):
                self.exercise(wrong=change)


if __name__ == '__main__':
    unittest.main()
