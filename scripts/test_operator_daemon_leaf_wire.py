import importlib.util
import json
from pathlib import Path
import shutil
import tempfile
import unittest
import operator_daemon_leaf_wire as wire

REPO=Path(__file__).resolve().parents[1]
FIXTURE=REPO/'docs/scale/operator-daemon-leaf-2026-10-07/wire-controls'
spec=importlib.util.spec_from_file_location('operator_runner',REPO/'scripts/run-operator-domain-controls.py')
runner=importlib.util.module_from_spec(spec);spec.loader.exec_module(runner)

class DaemonLeafWireControls(unittest.TestCase):
    def test_complete_actual_native_capture(self):
        result=wire.validate(FIXTURE)
        self.assertEqual(result['actual_standalone_processes'],5)
        self.assertEqual(len(runner.verify_daemon_leaf_log((FIXTURE/'native.log').read_text())),5)

    def test_actual_capture_mutations(self):
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp)/'fixture';shutil.copytree(FIXTURE,root)
            paths=list(root.rglob('daemon.process.json'))
            startup=next(p for p in paths if json.loads(p.read_text())['stage']=='startup')
            running=next(p for p in paths if json.loads(p.read_text())['stage']=='running')
            fatal=next(p for p in paths if json.loads(p.read_text())['stage']=='fatal')
            cases=[
                (root/'leaf-proof.json',lambda x:x.update(scenario_passed=False)),
                (root/'leaf-proof.json',lambda x:x.update(local_streams_after=1)),
                (root/'leaf-proof.json',lambda x:x.update(leaf_id='OTHER')),
                (root/'leaf-proof.json',lambda x:x['hubs'][0].update(domain='OTHER')),
                (startup,lambda x:x.update(reaped=False)),
                (startup,lambda x:x.update(signal='interrupt')),
                (startup,lambda x:x.update(exit_code=1)),
                (startup,lambda x:x['pending_api'].update(disposition='forwarded')),
                (startup,lambda x:x['pending_api'].update(forwarded_bytes=1)),
                (startup,lambda x:x['pending_api'].update(subject='$JS.OTHER.API.INFO')),
                (startup,lambda x:x['pending_api'].update(packet='UElORw0K')),
                (running,lambda x:x.update(pending_api=json.loads(startup.read_text())['pending_api'])),
                (fatal,lambda x:x.update(exit_code=0)),
                (running.parent/'traffic.json',lambda x:x.update(truncated=True)),
                (running.parent/'traffic.json',lambda x:x['frames'].pop()),
                (running.parent/'traffic.json',lambda x:x['connections'][0].update(target='127.0.0.1:1')),
                (running.parent/'proxy-final.json',lambda x:x.update(accepted_connections=2)),
                (running.parent/'proxy-final.json',lambda x:x.update(upstream_dial_failures=1)),
                (running.parent/'proxy-final.json',lambda x:x.update(active_connections=1)),
                (running.parent/'proxy-final.json',lambda x:x.update(buffer_overflows=1)),
                (running.parent/'proxy-final.json',lambda x:x.update(server_to_client=x['server_to_client']+1)),
            ]
            for path,mutate in cases:
                before=path.read_bytes();value=json.loads(before);mutate(value)
                self.assertNotEqual(value,json.loads(before))
                path.write_text(json.dumps(value))
                with self.subTest(file=path.name,mutation=mutate):
                    with self.assertRaises(AssertionError):wire.validate(root)
                path.write_bytes(before)
            before=fatal.read_bytes();fatal.unlink()
            with self.assertRaises(AssertionError):wire.validate(root)
            fatal.write_bytes(before)

    def test_actual_log_mutations(self):
        log=(FIXTURE/'native.log').read_text()
        lines=[line for line in log.splitlines() if 'packaged leaf daemon:' in line]
        bad=[log+'DATA RACE',log.replace('--- PASS: '+wire.TEST,'--- SKIP: '+wire.TEST),log.replace('local=WFEDGE','local=WFOPS'),log.replace('stage=fatal','stage=running'),log.replace('exit=1','exit=0')]
        for line in lines:bad.extend((log.replace(line,''),log.replace(line,line+'\n'+line)))
        for value in bad:
            self.assertNotEqual(value,log)
            with self.assertRaises(AssertionError):runner.verify_daemon_leaf_log(value)

if __name__=='__main__':unittest.main()
