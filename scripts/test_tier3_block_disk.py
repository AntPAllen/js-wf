import json
from pathlib import Path
import tempfile
import unittest
from test_tier3_journal_row import row, fixture


class BlockDiskChecks(unittest.TestCase):
    def test_log_scope(self):
        events = fixture('35s')
        for event in events:
            if 'Test' in event: event['Test'] = row.TESTS['block_disk']
        events[0]['Output'] = events[0]['Output'].replace('row=journal', 'row=block_disk')
        self.assertFalse(row.check(events, '35s', 'block_disk')['clears_full_tier3_release'])

    def test_rejects_unproven_stalls(self):
        stamp = lambda s: f'2026-10-02T00:00:{s:02d}Z'
        for mode in ('valid', 'empty', 'short', 'sync_escaped', 'wrong_state', 'wrong_node',
                     'wrong_mount', 'read_only', 'late_binding', 'wrong_leader', 'missing_role',
                     'wrong_replication', 'late_role', 'reversed_heal'):
            with self.subTest(mode=mode), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                fault = dict(node=4, scheduled=stamp(30), killed=stamp(32), healed=stamp(39),
                             block_stall=dict(suspended=stamp(32), resumed=stamp(37),
                                              sync_returned=stamp(38), device_state='Suspended'))
                binding = dict(store_dir='/private/store', observed=stamp(30), binding=dict(
                    node=4, container='fixture-n4', source='/private/store', destination='/data', writable=True))
                roles = [dict(stage='before', fault=1, stream=name, observed=stamp(31), info=dict(
                    config=dict(name=name, num_replicas=5), cluster=dict(leader='fixture-n4', replicas=[{}]*4)))
                         for name in ('WF_RUN', 'WF_JRN')]
                if mode == 'short': fault['block_stall']['resumed'] = stamp(36)
                elif mode == 'sync_escaped': fault['block_stall']['sync_returned'] = stamp(33)
                elif mode == 'wrong_state': fault['block_stall']['device_state'] = 'Active'
                elif mode == 'wrong_node': fault['node'] = 3
                elif mode == 'wrong_mount': binding['binding']['source'] = '/other/store'
                elif mode == 'read_only': binding['binding']['writable'] = False
                elif mode == 'late_binding': binding['observed'] = stamp(33)
                elif mode == 'wrong_leader': roles[0]['info']['cluster']['leader'] = 'fixture-n3'
                elif mode == 'missing_role': roles.pop()
                elif mode == 'wrong_replication': roles[0]['info']['config']['num_replicas'] = 3
                elif mode == 'late_role': roles[0]['observed'] = stamp(33)
                elif mode == 'reversed_heal': fault['healed'] = stamp(37)
                for name, data in [('faults.json', [] if mode == 'empty' else [fault]),
                                   ('fault-1-block-disk-binding.json', binding),
                                   ('fault-1-block-disk-roles.json', roles)]:
                    (root/name).write_text(json.dumps(data))
                if mode == 'valid':
                    report = row.check_block_disk_artifacts(root, dict(confirmed_faults=1))
                    self.assertEqual(report['confirmed_five_second_stalls'], 1)
                    self.assertFalse(report['confirms_dm_delay'])
                else:
                    with self.assertRaises(ValueError):
                        row.check_block_disk_artifacts(root, dict(confirmed_faults=1))
