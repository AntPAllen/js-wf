import base64
import json
from pathlib import Path
import shutil
import tempfile
import unittest
from sql_leaf_stream_wire import validate_phase,ProtocolStream

FIXTURE=Path(__file__).resolve().parents[1]/'docs/scale/postgres-leaf-projection-50000-2026-10-07/wire-controls/initial'

class SQLLeafStreamingWireControls(unittest.TestCase):
    def test_actual_closed_initial_capture(self):
        result=validate_phase(FIXTURE)
        self.assertEqual(result['frame_records'],22)
        self.assertEqual(result['streams']['client_to_server']['bytes'],1738)
        self.assertEqual(result['streams']['server_to_client']['bytes'],11594)
        # Feed actual captured bytes one at a time, exercising every split.
        streams={d:ProtocolStream(d=='server_to_client') for d in ('client_to_server','server_to_client')}
        for line in (FIXTURE/'traffic.frames.jsonl').read_text().splitlines():
            row=json.loads(line)
            for value in base64.b64decode(row['data']):streams[row['direction']].feed(bytes([value]))
        for direction,stream in streams.items():
            actual=stream.finish();expected=result['streams'][direction]
            self.assertEqual(actual['sha256'],expected['sha256']);self.assertEqual(actual['operations'],expected['operations'])

    def test_actual_capture_mutations(self):
        positive=validate_phase(FIXTURE);leaf_id=positive['streams']['server_to_client']['infos'][0]['server_id']
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp)/'initial';shutil.copytree(FIXTURE,root)
            cases=[('traffic.json',lambda x:x.update(truncated=True)),('traffic.json',lambda x:x.update(frame_file_error='failed')),
                ('traffic.json',lambda x:x.update(frame_records=x['frame_records']+1)),('traffic.json',lambda x:x.update(frame_file='../other')),
                ('traffic.json',lambda x:x.update(frames=[{}])),('traffic.json',lambda x:x.update(connections=[])),
                ('proxy-final.json',lambda x:x.update(accepted_connections=2)),('proxy-final.json',lambda x:x.update(accepted_connections=True)),
                ('proxy-final.json',lambda x:x.update(upstream_dial_failures=1)),('proxy-final.json',lambda x:x.update(active_connections=1)),
                ('proxy-final.json',lambda x:x.update(buffered_bytes=1)),('proxy-final.json',lambda x:x.update(buffer_overflows=1)),
                ('proxy-final.json',lambda x:x.update(client_to_server=x['client_to_server']+1))]
            for name,alter in cases:
                p=root/name;before=p.read_bytes();value=json.loads(before);alter(value);self.assertNotEqual(json.dumps(value,sort_keys=True),json.dumps(json.loads(before),sort_keys=True));p.write_text(json.dumps(value))
                with self.subTest(file=name,mutation=alter):
                    with self.assertRaises(AssertionError):validate_phase(root,leaf_id)
                p.write_bytes(before)
            p=root/'traffic.frames.jsonl';before=p.read_bytes();rows=[json.loads(line) for line in before.splitlines()]
            changed=[]
            for label,alter in [('connection',lambda x:x[0].update(connection=99)),('direction',lambda x:x[0].update(direction='unknown')),('missing record',lambda x:x.pop())]:
                value=json.loads(json.dumps(rows));alter(value);changed.append((label,b''.join((json.dumps(row)+'\n').encode() for row in value)))
            value=json.loads(json.dumps(rows));found=False
            for row in value:
                data=base64.b64decode(row['data'])
                if row['direction']=='client_to_server' and b'$JS.WFVIEW.API.' in data:
                    row['data']=base64.b64encode(data.replace(b'$JS.WFVIEW.API.',b'$JS.OTHERX.API.')).decode();found=True
            self.assertTrue(found);changed.append(('same-byte-length wrong API prefix',b''.join((json.dumps(row)+'\n').encode() for row in value)))
            value=json.loads(json.dumps(rows));found=False
            for row in value:
                data=base64.b64decode(row['data'])
                if row['direction']=='server_to_client' and leaf_id.encode() in data:
                    row['data']=base64.b64encode(data.replace(leaf_id.encode(),b'X'*len(leaf_id))).decode();found=True
            self.assertTrue(found);changed.append(('same-byte-length wrong INFO identity',b''.join((json.dumps(row)+'\n').encode() for row in value)))
            changed.extend((('encoded tail',before[:-1]),('dropped encoded bytes',before[:-25])))
            for label,data in changed:
                self.assertNotEqual(data,before);p.write_bytes(data)
                with self.subTest(mutation=label):
                    with self.assertRaises((AssertionError,ValueError)):validate_phase(root,leaf_id)
                p.write_bytes(before)

    def test_framing_limits_and_incomplete_tail(self):
        for data in (b'PUB s 16777217\r\n',b'PUB s -1\r\n',b'HPUB s 4 2\r\n',b'PUB s 1\r\nxXX',b'X'*(65537)):
            stream=ProtocolStream()
            with self.assertRaises(AssertionError):stream.feed(data)
        stream=ProtocolStream();stream.feed(b'PUB s 2\r\nx')
        with self.assertRaises(AssertionError):stream.finish()

if __name__=='__main__':unittest.main()
