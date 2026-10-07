"""Bounded interrupted-tail accounting; healthy traces remain strict."""
import unittest,sys,hashlib
sys.dont_write_bytecode=True
from sql_leaf_stream_wire import ProtocolStream

class FaultTailTests(unittest.TestCase):
 def test_fault_tail_accounted_for_every_byte_split(self):
  payload=b'INFO {"server_id":"actual","domain":"WFEDGE"}\r\nMSG inbox 1 10\r\n12345'
  for split in range(len(payload)+1):
   stream=ProtocolStream(True);stream.feed(payload[:split]);stream.feed(payload[split:]);result=stream.finish(allow_tail=True)
   self.assertEqual(result['bytes'],len(payload));self.assertEqual(result['sha256'],hashlib.sha256(payload).hexdigest());self.assertEqual(result['interrupted_tail_bytes'],len(b'MSG inbox 1 10\r\n12345'));self.assertEqual(result['interrupted_tail_sha256'],hashlib.sha256(b'MSG inbox 1 10\r\n12345').hexdigest());self.assertEqual(result['operations'],{'INFO':1})
 def test_healthy_traces_reject_same_tail(self):
  stream=ProtocolStream(True);stream.feed(b'INFO {"server_id":"actual"}\r\nMSG inbox 1 10\r\n12345')
  with self.assertRaises(AssertionError):stream.finish()
 def test_fault_mode_still_rejects_invalid_packets_and_domains(self):
  for incoming,data in [(True,b'INFO {}\r\nGARBAGE'),(True,b'INFO {}\r\nMSG inbox 1 9999999999\r\n'),(True,b'INFO {}\r\nMSG inbox 1 1\r\nxZZ'),(False,b'PUB $JS.WRONG.API.INFO inbox 2\r\n{}\r\n'),(False,b'NOTAPACKET')]:
   with self.assertRaises(AssertionError):
    stream=ProtocolStream(incoming);stream.feed(data);stream.finish(allow_tail=True)
if __name__=='__main__':unittest.main()
