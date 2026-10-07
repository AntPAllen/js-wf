#!/usr/bin/env python3
"""Start the complete Raft comparison in a retained user systemd service.

The native runner retains its original per-SDK deadlines and resource profile.
Service state and append-only logs survive the invoking tool session.
"""
import argparse
import json
import os
from pathlib import Path
import re
import subprocess
import sys


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--root', type=Path, required=True)
    p.add_argument('--parent-root', type=Path, required=True)
    p.add_argument('--unit', required=True)
    p.add_argument('--log', type=Path, required=True)
    p.add_argument('--receipt', type=Path, required=True)
    p.add_argument('--guard-variant', choices=['strict', 'contiguous'], default='strict')
    p.add_argument('--test-profile', choices=['original', 'locked-peer-setup', 'synchronized-setup'], default='original')
    a = p.parse_args()
    repo = Path(__file__).resolve().parents[1]
    if not re.fullmatch(r'[A-Za-z0-9_-]+', a.unit):
        p.error('require a simple fresh unit name without a suffix')
    if not all(v.is_absolute() for v in [a.root, a.parent_root, a.log, a.receipt]):
        p.error('require absolute paths')
    if a.root.exists() or a.log.exists() or a.receipt.exists():
        p.error('require fresh root, log and service receipt')
    if a.root.is_relative_to(repo) or a.log.is_relative_to(a.root) or a.receipt.is_relative_to(a.root):
        p.error('require fixture outside checkout and service records outside fixture')
    if not a.parent_root.is_dir() or a.parent_root.is_symlink():
        p.error('require a regular preserved parent directory')
    unit = a.unit + '.service'
    if subprocess.run(['systemctl', '--user', 'cat', unit], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL).returncode == 0:
        p.error('unit already exists; inspect it rather than restarting')
    command = ['systemd-run', '--user', '--unit=' + a.unit,
               '--property=Type=exec', '--property=RemainAfterExit=yes',
               '--property=Restart=no', '--property=RuntimeMaxSec=50min',
               '--property=WorkingDirectory=' + str(repo),
               '--property=StandardOutput=append:' + str(a.log),
               '--property=StandardError=inherit', '--setenv=PATH=' + os.environ['PATH'],
               '--setenv=PYTHONDONTWRITEBYTECODE=1',
               sys.executable, str(repo/'scripts/run-raft-catchup-safety-controls.py'),
               '--root', str(a.root), '--parent-root', str(a.parent_root),
               '--guard-variant', a.guard_variant, '--test-profile', a.test_profile]
    subprocess.run(command, cwd=repo, check=True)
    observed = subprocess.check_output(['systemctl', '--user', 'show', unit,
                                       '--property=ActiveState,SubState,MainPID,ExecMainPID,ExecMainCode,ExecMainStatus,Result,RuntimeMaxUSec,Restart'], text=True)
    record = dict(unit=unit, command=command, initial_service_state=observed,
                  source=subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd=repo, text=True).strip(),
                  scope='Durable execution only. Original complete170/count1/20m per SDK/2CPU/2GiB retained; 50-minute service bound covers both SDK deadlines plus collection. No automatic restart or qualification inferred from service startup.')
    a.receipt.write_text(json.dumps(record, indent=2) + '\n')
    print(json.dumps(dict(unit=unit, receipt=str(a.receipt))))


if __name__ == '__main__':
    main()
