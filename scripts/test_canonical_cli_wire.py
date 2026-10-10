import base64
import json
from pathlib import Path
import tempfile
import random
import unittest
from canonical_cli_wire import ProtocolStream,validate

class CanonicalCLIProtocolTests(unittest.TestCase):
    def test_payload_cannot_masquerade_as_api(self):
        body=b'PUB $JS.WRONG.API.STREAM.INFO.WF_JRN 0\r\n\r\n'
        wire=b'CONNECT {}\r\nPUB $JS.WFGRAPHOPS.API.STREAM.INFO.AUTH '+str(len(body)).encode()+b'\r\n'+body+b'\r\nPING\r\n'
        for seed in (1,42,853):
            with self.subTest(seed=seed):
                rng=random.Random(seed);p=ProtocolStream(api_prefix='$JS.WFGRAPHOPS.API.')
                pos=0
                while pos<len(wire):
                    size=rng.randint(1,13);p.feed(wire[pos:pos+size]);pos+=size
                result=p.finish()
                self.assertEqual(result['api'],{'$JS.WFGRAPHOPS.API.STREAM.INFO.AUTH':1})
                self.assertEqual(result['operations'],{'CONNECT':1,'PUB':1,'PING':1})
    def test_wrong_domain_rejected(self):
        p=ProtocolStream(api_prefix='$JS.WFGRAPHOPS.API.')
        with self.assertRaises(AssertionError):p.feed(b'PUB $JS.API.STREAM.INFO.AUTH 0\r\n\r\n')
    def test_incomplete_payload_rejected(self):
        p=ProtocolStream(api_prefix='$JS.API.');p.feed(b'PUB $JS.API.STREAM.INFO.AUTH 3\r\nx')
        with self.assertRaises(AssertionError):p.finish()
    def test_bad_payload_terminator_rejected(self):
        p=ProtocolStream(api_prefix='$JS.API.')
        with self.assertRaises(AssertionError):p.feed(b'PUB $JS.API.STREAM.INFO.AUTH 1\r\nx!!')
    def test_oversized_payload_rejected(self):
        p=ProtocolStream(api_prefix='$JS.API.')
        with self.assertRaises(AssertionError):p.feed(b'PUB $JS.API.STREAM.INFO.AUTH 16777217\r\n')
    def test_incoming_starts_with_info(self):
        p=ProtocolStream(True,'$JS.API.')
        with self.assertRaises(AssertionError):p.feed(b'PING\r\n')

class CanonicalCleanupTests(unittest.TestCase):
    def capture(self,directory,filter='wf.jrn.graph-operator.success',extra=b'',duplicate=False):
        payload=json.dumps({'filter':filter}).encode()
        if duplicate:payload=b'{"filter":"wrong","filter":"wf.jrn.graph-operator.success"}'
        prefix=b'$JS.WFGRAPHOPS.API.'
        request=b'CONNECT {}\r\nPUB '+prefix+b'STREAM.INFO.WF_JRN 0\r\n\r\nPUB '+prefix+b'STREAM.PURGE.WF_JRN '+str(len(payload)).encode()+b'\r\n'+payload+b'\r\n'+extra
        reply=b'INFO {"server_id":"N","domain":"WFGRAPHOPS"}\r\n'
        frames=[dict(connection=1,direction=d,data=base64.b64encode(data).decode()) for d,data in [('client_to_server',request),('server_to_client',reply)]]
        root=Path(directory)
        (root/'traffic.frames.jsonl').write_text(''.join(json.dumps(f)+'\n' for f in frames))
        (root/'traffic.json').write_text(json.dumps(dict(truncated=False,frame_file='traffic.frames.jsonl',frame_records=2,frames=None,connections=[dict(id=1)])))
        (root/'proxy-final.json').write_text(json.dumps(dict(active_connections=0,buffered_bytes=0,buffer_overflows=0,upstream_dial_failures=0,accepted_connections=1,client_to_server=len(request),server_to_client=len(reply))))
        return root
    def test_only_exact_explicit_cleanup_allowed(self):
        with tempfile.TemporaryDirectory() as directory:
            root=self.capture(directory)
            with self.assertRaises(AssertionError):validate(root,'WFGRAPHOPS')
            result=validate(root,'WFGRAPHOPS',legacy_purge_subject='wf.jrn.graph-operator.success')
            self.assertEqual(result['streams']['client_to_server']['legacy_purges'],[{'filter':'wf.jrn.graph-operator.success'}])
    def test_offline_omits_zero_frame_counter(self):
        with tempfile.TemporaryDirectory() as directory:
            root=self.capture(directory)
            (root/'traffic.frames.jsonl').write_text('')
            (root/'traffic.json').write_text(json.dumps(dict(truncated=False,frame_file='traffic.frames.jsonl',frames=None,connections=None)))
            (root/'proxy-final.json').write_text(json.dumps(dict(active_connections=0,buffered_bytes=0,buffer_overflows=0,upstream_dial_failures=0,accepted_connections=0,client_to_server=0,server_to_client=0)))
            self.assertTrue(validate(root,'WFGRAPHOPS',offline=True)['offline'])
    def test_other_target_rejected(self):
        with tempfile.TemporaryDirectory() as directory:
            root=self.capture(directory,filter='wf.jrn.foreign.*')
            with self.assertRaises(AssertionError):validate(root,'WFGRAPHOPS',legacy_purge_subject='wf.jrn.graph-operator.success')
    def test_history_read_not_cleanup(self):
        with tempfile.TemporaryDirectory() as directory:
            root=self.capture(directory,extra=b'PUB $JS.WFGRAPHOPS.API.STREAM.MSG.GET.WF_JRN 0\r\n\r\n')
            with self.assertRaises(AssertionError):validate(root,'WFGRAPHOPS',legacy_purge_subject='wf.jrn.graph-operator.success')
    def test_duplicate_purge_field_rejected(self):
        with tempfile.TemporaryDirectory() as directory:
            root=self.capture(directory,duplicate=True)
            with self.assertRaises(AssertionError):validate(root,'WFGRAPHOPS',legacy_purge_subject='wf.jrn.graph-operator.success')

if __name__=='__main__':unittest.main()
