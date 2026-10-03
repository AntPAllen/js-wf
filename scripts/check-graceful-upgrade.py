#!/usr/bin/env python3
"""Check recorded Lame Duck entry, clean exit and exact-container removal."""
import importlib.util
from pathlib import Path

spec=importlib.util.spec_from_file_location('upgrade_timestamps',Path(__file__).with_name('check-tier3-journal-row.py'))
timestamps=importlib.util.module_from_spec(spec)
spec.loader.exec_module(timestamps)

def check(proof,node):
    if proof.get('node')!=node or proof.get('signal')!='SIGUSR2' or not proof.get('container') or proof.get('version')!='2.11.17' or not proof.get('server_id'):
        raise ValueError('graceful upgrade lacks original peer identity and documented signal')
    started,returned,entered,stopped,cleanup=[timestamps.timestamp_ns(proof[k]) for k in ('signal_started','signal_returned','lame_duck_observed','source_stopped','cleanup_complete')]
    if not started<=entered<=stopped<=cleanup or not started<=returned<=cleanup or cleanup-started>80_000_000_000 or proof.get('state') not in ('absent','exited','dead'):
        raise ValueError('graceful upgrade notification/exit timeline is invalid')
    logs=proof.get('shutdown_logs','')
    markers=['Entering lame duck mode','Initiating Shutdown...','Server Exiting..']
    positions=[logs.find(marker) for marker in markers]
    if any(p<0 for p in positions) or positions!=sorted(positions):
        raise ValueError('graceful upgrade lacks ordered original clean shutdown logs')
    return dict(node=node,server_id=proof['server_id'],shutdown_seconds=(cleanup-started)/1e9,clean_shutdown=True)
