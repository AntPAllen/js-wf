import base64
import copy
import importlib.util
import json
from pathlib import Path
import tempfile
import unittest


def load(name, filename):
    spec = importlib.util.spec_from_file_location(name, Path(__file__).with_name(filename))
    module = importlib.util.module_from_spec(spec); spec.loader.exec_module(module); return module


checker = load('clock_cut_check', 'check-clock-timer-cut.py')
row = load('clock_cut_row', 'check-tier3-journal-row.py')


class ClockTimerCutChecks(unittest.TestCase):
    def fixture(self, root, direction):
        offset = -60_000_000_000 if direction == 'behind' else 60_000_000_000
        server = '2026-10-01T23:17:57Z' if direction == 'behind' else '2026-10-01T23:19:57Z'
        fire = server.replace('57Z', '57.250Z')
        stamp = lambda fraction: '2026-10-01T23:18:57.'+fraction+'Z'
        subject = 'wf.jrn.matrixtimer.inv'
        request = dict(sequence=11, subject=subject, observed_at=stamp('010'), entry=dict(index=16, kind='StepRequested', worker_id='worker', payload=dict(kind='timer', name='wait', duration_nanos=250_000_000, fire_at=fire)))
        suspended = dict(sequence=12, subject=subject, observed_at=stamp('020'), entry=dict(index=17, kind='Suspended', worker_id='worker', payload=dict(waiting_on='timer:wait')))
        origin = dict(Operation='timer_clock', Type='matrixtimer', ID='inv', Worker='worker', JournalIndex=16, JournalKind='StepRequested', Error='', Duration=1_000_000, At=stamp('001'), ServerTime=server)
        admission = dict(id='inv', request=request, suspended=suspended, origin=origin, observed=stamp('030'), earliest_due=stamp('250'), source_offset_ns=offset)
        cut = dict(admission=admission, refreshed_tail=dict(Subject=subject, Sequence=12, Data=base64.b64encode(json.dumps(suspended['entry']).encode()).decode()), refreshed=stamp('040'), removed=stamp('060'))
        files = {'faults.json':[dict(node=4,killed=stamp('050'))], 'controller-receipts.json':[request,suspended], 'controller-operations.json':[origin], 'controller-latency-audit.json':dict(bounds=[dict(sequence=11,type="matrixtimer",id="inv",entry=request['entry']),dict(sequence=12,type="matrixtimer",id="inv",entry=suspended['entry']),dict(sequence=13,type='matrixtimer',id='inv',before=stamp('300'))]), 'fault-1-journal-operations.json':[dict(at=stamp('060'))], 'fault-1-clock-timer-cut.json':cut}
        for name, data in files.items(): (root/name).write_text(json.dumps(data))
        return files

    def test_both_directions_and_missing_or_corrupt_proofs(self):
        for direction in ('ahead','behind'):
            for mode in ('valid','missing','changed_receipt','changed_tail','wrong_origin','wrong_source','wrong_due','late_removal','overlap','missing_final_prefix'):
                with self.subTest(direction=direction,mode=mode), tempfile.TemporaryDirectory() as directory:
                    root = Path(directory); files = self.fixture(root,direction); cut = files['fault-1-clock-timer-cut.json']
                    if mode == 'missing': (root/'fault-1-clock-timer-cut.json').unlink()
                    elif mode == 'changed_receipt': cut['admission']['request'] = copy.deepcopy(cut['admission']['request']); cut['admission']['request']['sequence']=99
                    elif mode == 'changed_tail': cut['refreshed_tail']['Sequence']=13
                    elif mode == 'wrong_origin': cut['admission']['origin']=copy.deepcopy(cut['admission']['origin']); cut['admission']['origin']['Error']='unknown'
                    elif mode == 'wrong_source': cut['admission']['source_offset_ns']=0
                    elif mode == 'wrong_due': cut['admission']['earliest_due']='2026-10-01T23:18:58Z'
                    elif mode == 'late_removal': cut['removed']='2026-10-01T23:18:58Z'
                    elif mode == 'overlap': files['controller-latency-audit.json']['bounds'][2]['before']='2026-10-01T23:18:57.059Z'
                    elif mode == 'missing_final_prefix': files['controller-latency-audit.json']['bounds'].pop(0)
                    for name,data in files.items():
                        if mode != 'missing' or name != 'fault-1-clock-timer-cut.json': (root/name).write_text(json.dumps(data))
                    if mode == 'valid':
                        result=checker.check(root,dict(confirmed_faults=1),'server_clock_'+direction,row.timestamp_ns)
                        self.assertEqual(result['admitted_pending_sleep_cuts'],1);self.assertFalse(result['clears_full_tier3_release'])
                    else:
                        with self.assertRaises((ValueError,KeyError,FileNotFoundError)):
                            checker.check(root,dict(confirmed_faults=1),'server_clock_'+direction,row.timestamp_ns)

    def test_canonical_domain_requires_acknowledged_shifted_translation(self):
        for direction in ('ahead','behind'):
            for mode in ('valid','missing_hint','wrong_domain','wrong_bounds','unshifted','wrong_translation','duplicate_ack','unknown_ack','ambiguous_hint','late_hint'):
                with self.subTest(direction=direction,mode=mode), tempfile.TemporaryDirectory() as directory:
                    root=Path(directory);files=self.fixture(root,direction)
                    admission=files['fault-1-clock-timer-cut.json']['admission']
                    origin=admission['origin'];server=origin.pop('ServerTime')
                    origin.update(Operation='timer_domain_clock',ClockDomain='utc-quorum-v1',ClockLower='2026-10-01T23:18:57Z',ClockUpper='2026-10-01T23:18:57Z')
                    payload=admission['request']['entry']['payload'];payload.update(clock_domain='utc-quorum-v1',fire_at='2026-10-01T23:18:57.250Z')
                    hint=copy.deepcopy(origin);hint.update(Operation='timer_native_hint',JournalKind='StepRequested',ServerTime=server,TimerPublished=True,TimerDeadline=payload['fire_at'],TimerScheduleAt=('2026-10-01T23:19:57.250Z' if direction=='ahead' else '2026-10-01T23:17:57.250Z'))
                    admission['native_hint']=hint;files['controller-operations.json'].append(hint)
                    if mode=='missing_hint':admission.pop('native_hint')
                    elif mode=='wrong_domain':origin['ClockDomain']='unknown'
                    elif mode=='wrong_bounds':origin['ClockUpper']='2026-10-01T23:19:57Z'
                    elif mode=='unshifted':hint['ServerTime']='2026-10-01T23:18:57Z'
                    elif mode=='wrong_translation':hint['TimerScheduleAt']='2026-10-01T23:18:57.251Z'
                    elif mode=='duplicate_ack':hint['TimerPublished']=False
                    elif mode=='unknown_ack':hint['Error']='outcome unknown'
                    elif mode=='ambiguous_hint':files['controller-operations.json'].append(copy.deepcopy(hint))
                    elif mode=='late_hint':hint['At']='2026-10-01T23:18:58Z'
                    for name,data in files.items():(root/name).write_text(json.dumps(data))
                    if mode=='valid':self.assertEqual(checker.check(root,dict(confirmed_faults=1),'server_clock_'+direction,row.timestamp_ns)['admitted_pending_sleep_cuts'],1)
                    else:
                        with self.assertRaises((ValueError,KeyError)):checker.check(root,dict(confirmed_faults=1),'server_clock_'+direction,row.timestamp_ns)
