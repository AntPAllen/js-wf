import random
import unittest
from worker_leaf_wire import protocol

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

if __name__=='__main__':unittest.main()
