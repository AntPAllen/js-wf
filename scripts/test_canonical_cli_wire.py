import random
import unittest
from canonical_cli_wire import ProtocolStream

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

if __name__=='__main__':unittest.main()
