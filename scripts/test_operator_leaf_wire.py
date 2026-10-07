import base64
import json
from pathlib import Path
import shutil
import tempfile
import unittest
import operator_leaf_wire as wire

REPO=Path(__file__).resolve().parents[1]
FIXTURE=REPO/'docs/scale/operator-leaf-wire-2026-10-07/wire-controls'


class OperatorLeafWireTests(unittest.TestCase):
    def test_actual_complete_capture_and_offline_children(self):
        result=wire.validate(FIXTURE)
        self.assertEqual((result['online_children'],result['offline_children']),(21,2))
        self.assertEqual((result['client_bytes'],result['server_bytes'],result['domain_api_publications']),(48925, 3874289, 313))

    def test_actual_capture_rejects_missing_bytes_prefix_bypass_and_identity_changes(self):
        with tempfile.TemporaryDirectory() as temp:
            root=Path(temp)/'fixture';shutil.copytree(FIXTURE,root)
            online=next(p.parent for p in root.rglob('wire-expectation.json') if not json.loads(p.read_text())['offline'] and 'MISSING' not in json.loads(p.read_text())['args'])
            offline=next(p.parent for p in root.rglob('wire-expectation.json') if json.loads(p.read_text())['offline'])
            cases=[]
            def add(path,mutate):cases.append((path,mutate))
            add(root/'leaf-proof.json',lambda x:x.update(scenario_passed=False))
            add(root/'leaf-proof.json',lambda x:x.update(local_streams_after=1))
            add(root/'leaf-proof.json',lambda x:x.update(leaf_id='OTHER'))
            add(root/'leaf-proof.json',lambda x:x['hubs'][0].update(domain='OTHER'))
            add(online/'traffic.json',lambda x:x.update(truncated=True))
            add(online/'traffic.json',lambda x:x['frames'].pop())
            add(online/'traffic.json',lambda x:x['connections'][0].update(target='127.0.0.1:1'))
            add(online/'proxy-final.json',lambda x:x.update(active_connections=1))
            add(online/'proxy-final.json',lambda x:x.update(accepted_connections=2))
            add(online/'proxy-final.json',lambda x:x.update(upstream_dial_failures=1))
            add(offline/'proxy-final.json',lambda x:x.update(accepted_connections=1))
            add(offline/'proxy-final.json',lambda x:x.update(upstream_dial_failures=1))
            add(online/'proxy-final.json',lambda x:x.update(server_to_client=x['server_to_client']+1))
            add(online/'wire-expectation.json',lambda x:x.update(offline=True))
            add(offline/'traffic.json',lambda x:x.update(connections=[dict(id=1,target='127.0.0.1:1')]))
            add(offline/'proxy-final.json',lambda x:x.update(client_to_server=1))
            def prefix(x):
                changed=False
                for frame in x['frames']:
                    if frame['direction']=='client_to_server':
                        data=base64.b64decode(frame['data'])
                        if b'$JS.WFOPS.API.' in data:
                            frame['data']=base64.b64encode(data.replace(b'$JS.WFOPS.API.',b'$JS.OTHER.API.')).decode();changed=True
                self.assertTrue(changed)
            add(online/'traffic.json',prefix)
            for path,mutate in cases:
                before=path.read_bytes();value=json.loads(before);mutate(value)
                with self.subTest(file=path.name,mutation=mutate):
                    self.assertNotEqual(json.loads(before),value)
                    path.write_text(json.dumps(value))
                    with self.assertRaises(AssertionError):wire.validate(root)
                path.write_bytes(before)
            leaf=root/'leaf-proof.json';data=leaf.read_bytes();leaf.unlink()
            with self.assertRaises(AssertionError):wire.validate(root)
            leaf.write_bytes(data)


if __name__=='__main__':unittest.main()
