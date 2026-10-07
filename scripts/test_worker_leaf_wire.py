import random
import copy
import json
from pathlib import Path
import tempfile
import unittest
from worker_leaf_wire import protocol,validate

class WireFramingControls(unittest.TestCase):
    def test_payload_protocol_text_and_arbitrary_tcp_boundaries(self):
        payload=b'PUB $JS.API.STREAM.INFO.BAD 0\r\n\r\n'
        stream=(b'CONNECT {"verbose":false}\r\nSUB _INBOX.x 1\r\n'
                +b'PUB application '+str(len(payload)).encode()+b'\r\n'+payload+b'\r\n'
                +b'HPUB $JS.WFWORKER.API.STREAM.INFO.WF_JRN _INBOX.x 12 14\r\nNATS/1.0\r\n\r\n{}\r\nPING\r\n')
        expected=protocol(stream)
        self.assertEqual(protocol(bytearray(stream)),expected)
        self.assertEqual(protocol(memoryview(stream)),expected)
        self.assertEqual([parts[1] for op,parts,_ in expected if op in (b'PUB',b'HPUB')],
                         [b'application',b'$JS.WFWORKER.API.STREAM.INFO.WF_JRN'])
        self.assertEqual(expected[2][2],payload)
        for seed in range(64):
            rng=random.Random(seed);chunks=[];offset=0
            while offset<len(stream):
                size=rng.randint(1,17);chunks.append(stream[offset:offset+size]);offset+=size
            self.assertEqual(protocol(b''.join(chunks)),expected)

    def test_incoming_payload_is_not_info(self):
        stream=b'INFO {"server_id":"leaf","domain":"WFEDGE"}\r\nMSG x 1 4\r\nINFO\r\nPONG\r\n'
        parsed=protocol(stream,True)
        self.assertEqual([op for op,_,_ in parsed],[b'INFO',b'MSG',b'PONG'])

    def test_incomplete_or_malformed_frames_never_qualify(self):
        malformed=[b'PING',b'PUB x 3\r\nab\r\n',b'PUB x -1\r\n',
                   b'HPUB x 5 4\r\nabcd\r\n',b'PUB x 2\r\nabxx',b'CONNECT []\r\n',
                   b'INFO {}\r\n',b'UNKNOWN x\r\n',b'SUB x\r\n',b'PONG x\r\n']
        for data in malformed:
            with self.subTest(data=data),self.assertRaises((AssertionError,ValueError)):
                protocol(data)
        with self.assertRaises(AssertionError):protocol(b'-ERR bad\r\n',True)

class CapturedWorkerWireControls(unittest.TestCase):
    def test_actual_static_transcript_and_omission_bypass_controls(self):
        root=Path(__file__).resolve().parents[1]/'docs/scale/worker-leaf-wire-2026-10-07/native-race/wire-control-static'
        self.assertEqual(validate(root)['domain_api_publications'],1366)
        baseline={name:json.loads((root/name).read_text()) for name in ('leaf-proof.json','traffic.json','proxy-final.json')}
        variants=[lambda v:v['leaf-proof.json'].update(scenario_passed=False),
                  lambda v:v['leaf-proof.json'].update(remote_domain='WFEDGE'),
                  lambda v:v['leaf-proof.json'].update(local_streams_after=1),
                  lambda v:v['traffic.json'].update(truncated=True),
                  lambda v:v['traffic.json']['connections'][0].update(target='127.0.0.1:1'),
                  lambda v:v['traffic.json']['connections'].append(copy.deepcopy(v['traffic.json']['connections'][0])),
                  lambda v:v['traffic.json']['frames'][0].update(connection=999),
                  lambda v:v['proxy-final.json'].update(active_connections=1),
                  lambda v:v['proxy-final.json'].update(buffered_bytes=1),
                  lambda v:v['proxy-final.json'].update(buffer_overflows=1),
                  lambda v:v['proxy-final.json'].update(client_to_server=v['proxy-final.json']['client_to_server']+1),
                  lambda v:v['proxy-final.json'].update(server_to_client=v['proxy-final.json']['server_to_client']+1)]
        import base64
        def replace_wire(value,direction,old,new):
            found=False
            for frame in value['traffic.json']['frames']:
                if frame['direction']!=direction:continue
                data=base64.b64decode(frame['data']);changed=data.replace(old,new)
                found=found or changed!=data
                frame['data']=base64.b64encode(changed).decode()
            self.assertTrue(found)
        variants.extend([lambda v:replace_wire(v,'client_to_server',b'$JS.WFWORKER.API.',b'$JS.WRONGXXX.API.'),
                         lambda v:replace_wire(v,'server_to_client',b'WFEDGE',b'WRONG!'),
                         lambda v:replace_wire(v,'client_to_server',b'CONNECT ',b'UNKNOWN ')])
        with tempfile.TemporaryDirectory() as directory:
            target=Path(directory)
            for i,change in enumerate(variants):
                value=copy.deepcopy(baseline);change(value)
                for name,document in value.items():(target/name).write_text(json.dumps(document))
                with self.subTest(mutation=i),self.assertRaises((AssertionError,ValueError,KeyError)):validate(target)

if __name__=='__main__':unittest.main()
