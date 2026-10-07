"""Admit a spawned Linux process from stable observed identity, never intent.

A transient empty /proc environment during exec is an observation to repeat.
The caller's existing test-process timeout bounds the wait; this helper neither
kills nor restarts a process because an observation was incomplete.
"""
import hashlib
import os
from pathlib import Path
import time


def snapshot(pid, profile):
    proc = Path('/proc', str(pid))
    raw = dict(row.split(b'=', 1) for row in (proc/'environ').read_bytes().split(b'\0') if b'=' in row)
    stat = (proc/'stat').read_text()
    return dict(pid=pid, stat=stat, start_ticks=stat.rsplit(') ', 1)[1].split()[19],
                args=[os.fsdecode(row) for row in (proc/'cmdline').read_bytes().split(b'\0') if row],
                exe=str((proc/'exe').resolve()), working_directory=str((proc/'cwd').resolve()),
                environment={key:os.fsdecode(raw[key.encode()]) for key in profile if key.encode() in raw})


def admit(child, command, profile, cwd, binary, expected_sha256):
    observations, first_missing, rejected = 0, None, 0
    identity = lambda row: {key:row[key] for key in ('pid','start_ticks','args','exe','working_directory','environment')}
    while child.poll() is None:
        observations += 1
        try:
            observed = snapshot(child.pid, profile)
            missing = sorted(set(profile) - set(observed['environment']))
            if first_missing is None:
                first_missing = missing
            compatible = (not missing and observed['environment'] == profile and observed['args'] == command
                          and observed['working_directory'] == str(Path(cwd).resolve())
                          and observed['exe'] == str(Path(binary).resolve()))
            if compatible:
                with Path('/proc', str(child.pid), 'exe').open('rb') as stream:
                    digest = hashlib.file_digest(stream, 'sha256').hexdigest()
                confirmed = snapshot(child.pid, profile)
                if digest == expected_sha256 and identity(observed) == identity(confirmed) and child.poll() is None:
                    confirmed['exe_sha256'] = digest
                    confirmed['admission'] = dict(observations=observations, rejected_observations=rejected,
                        first_missing_environment_keys=first_missing, stable_identity_observed_twice=True,
                        scope='Actual live process identity before/after executable digest; incomplete reads repeated without killing or relaunching.')
                    return confirmed
        except (FileNotFoundError, ProcessLookupError):
            pass
        rejected += 1
        time.sleep(.01)
    raise RuntimeError(f'SDK exited before stable identity admission: exit={child.returncode}, observations={observations}')
