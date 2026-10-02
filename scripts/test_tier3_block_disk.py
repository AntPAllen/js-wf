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
                     'wrong_replication', 'late_role', 'reversed_heal', 'sync_during_resume', 'short_resume_hold', 'sync_before_resume'):
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
                if mode in ('sync_during_resume','short_resume_hold','sync_before_resume'):
                    fault['block_stall']['resume_started'] = stamp(37)
                    fault['block_stall']['resumed'] = stamp(39)
                    fault['healed'] = stamp(40)
                if mode == 'short_resume_hold': fault['block_stall']['resume_started'] = stamp(36)
                elif mode == 'sync_before_resume': fault['block_stall']['sync_returned'] = stamp(36)
                elif mode == 'short': fault['block_stall']['resumed'] = stamp(36)
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
                if mode in ('valid','sync_during_resume'):
                    report = row.check_block_disk_artifacts(root, dict(confirmed_faults=1))
                    self.assertEqual(report['confirmed_five_second_stalls'], 1)
                    self.assertFalse(report['confirms_dm_delay'])
                else:
                    with self.assertRaises(ValueError):
                        row.check_block_disk_artifacts(root, dict(confirmed_faults=1))

class BlockDelayChecks(unittest.TestCase):
    def test_delay_table_and_sync_proof(self):
        stamp = lambda s: f'2026-10-02T00:00:{s:02d}Z'
        for mode in ('valid', 'short_interval', 'fast_sync', 'wrong_delay', 'wrong_target',
                     'wrong_restored_target', 'changed_backing', 'sync_after_clear', 'late_restore',
                     'wrong_kill', 'wrong_offset', 'truncated_table'):
            with self.subTest(mode=mode), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                proof = dict(applied=stamp(32), clearing=stamp(37), restored=stamp(38),
                             sync_started=stamp(33), sync_returned=stamp(34), delay_ns=100_000_000,
                             active_table='0 1048576 delay 7:4 0 100', restored_table='0 1048576 linear 7:4 0')
                fault = dict(node=4, scheduled=stamp(30), killed=stamp(32), healed=stamp(39), block_delay=proof)
                binding = dict(store_dir='/private/store', observed=stamp(30), binding=dict(
                    node=4, container='fixture-n4', source='/private/store', destination='/data', writable=True))
                roles = [dict(stage='before', fault=1, stream=name, observed=stamp(31), info=dict(
                    config=dict(name=name, num_replicas=5), cluster=dict(leader='fixture-n4', replicas=[{}]*4)))
                         for name in ('WF_RUN', 'WF_JRN')]
                if mode == 'short_interval': proof['clearing'] = stamp(36)
                elif mode == 'fast_sync': proof['sync_returned'] = proof['sync_started']
                elif mode == 'wrong_delay': proof['active_table'] = '0 1048576 delay 7:4 0 10'
                elif mode == 'wrong_target': proof['active_table'] = '0 1048576 linear 7:4 0 100'
                elif mode == 'wrong_restored_target': proof['restored_table'] = proof['active_table']
                elif mode == 'changed_backing': proof['restored_table'] = '0 1048576 linear 7:5 0'
                elif mode == 'sync_after_clear': proof['sync_returned'] = stamp(38)
                elif mode == 'late_restore': proof['restored'] = stamp(40)
                elif mode == 'wrong_kill': fault['killed'] = stamp(31)
                elif mode == 'wrong_offset': proof['active_table'] = '0 1048576 delay 7:4 10 100'
                elif mode == 'truncated_table': proof['active_table'] = '0 1048576 delay'
                for name, data in [('faults.json', [fault]), ('fault-1-block-disk-binding.json', binding),
                                   ('fault-1-block-disk-roles.json', roles)]:
                    (root/name).write_text(json.dumps(data))
                if mode == 'valid':
                    report = row.check_block_disk_artifacts(root, dict(confirmed_faults=1), True)
                    self.assertTrue(report['confirms_dm_delay'])
                    self.assertEqual(report['confirmed_five_second_delay_intervals'], 1)
                    self.assertFalse(report['clears_full_tier3_release'])
                else:
                    with self.assertRaises(ValueError): row.check_block_disk_artifacts(root, dict(confirmed_faults=1), True)
