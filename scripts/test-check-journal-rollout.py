#!/usr/bin/env python3
import base64
import copy
import importlib.util
import json
from pathlib import Path
import subprocess
import tempfile

repo = Path(__file__).resolve().parents[1]
spec = importlib.util.spec_from_file_location('checker', repo/'scripts/check-journal-rollout.py')
checker = importlib.util.module_from_spec(spec)
spec.loader.exec_module(checker)
with tempfile.TemporaryDirectory() as directory:
    root = Path(directory)
    subprocess.run(['protoc','--proto_path='+str(repo),'--python_out='+str(root),'protocol/v1/journal.proto'],check=True)
    spec = importlib.util.spec_from_file_location('vector_codec',root/'protocol/v1/journal_pb2.py')
    codec = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(codec)
    old = 'matrix-process-0-generation-0'
    new = 'matrix-process-0-generation-1'
    proto = codec.JournalRecord(version=1,entry=codec.JournalEntry(epoch=1,index=1,kind=codec.ENTRY_KIND_STEP_COMPLETED,worker_id=old,payload_json=b'{"result":42}'))
    terminal = {'epoch':2,'index':2,'kind':'Completed','worker_id':new,'payload':{'result':'NDI='}}
    def encoded(wire): return base64.b64encode(wire).decode()
    proof = {'type':'matrixshort','id':'a','protobuf_worker_entries':1,'json_worker_entries':1,'mixed_worker_entries':True,'records':[{'sequence':1,'wire_base64':encoded(b'WFJ\0'+proto.SerializeToString())},{'sequence':2,'wire_base64':encoded(json.dumps(terminal).encode())}]}
    for owner,pid,encoding in [(old,1,'protobuf-v1'),(new,2,'json')]:
        (root/(owner+'-encoding.json')).write_text(json.dumps({'pid':pid,'worker':owner,'encoding':encoding}))
    (root/'process-evidence.json').write_text(json.dumps([{'worker_id':old,'pid':1},{'worker_id':new,'pid':2}]))
    (root/'journal-rollout.json').write_text(json.dumps({'profile':'protobuf-to-json','mixed_invocations':1,'all_retained_wire_formats_verified':True}))
    path = root/'rollout-matrixshort-a.json'
    path.write_text(json.dumps(proof))
    assert checker.check(root,{'invocations':1})['mixed_invocations']==1
    cases=[]
    bad=copy.deepcopy(proof);bad['protobuf_worker_entries']=2;cases.append(bad)
    bad=copy.deepcopy(proof);bad['records'][1]['sequence']=1;cases.append(bad)
    bad=copy.deepcopy(proof);bad['records'][1]['wire_base64']='!';cases.append(bad)
    bad=copy.deepcopy(proof);changed=dict(terminal,worker_id=old);bad['records'][1]['wire_base64']=encoded(json.dumps(changed).encode());cases.append(bad)
    bad=copy.deepcopy(proof);changed=dict(terminal,worker_id='unknown');bad['records'][1]['wire_base64']=encoded(json.dumps(changed).encode());cases.append(bad)
    for bad in cases:
        path.write_text(json.dumps(bad))
        try: checker.check(root,{'invocations':1})
        except (ValueError,KeyError): pass
        else: raise AssertionError('incorrect rollout accepted')
    path.write_text(json.dumps(proof))
    try: checker.check(root,{'invocations':2})
    except ValueError: pass
    else: raise AssertionError('missing invocation accepted')
print('PASS journal rollout verifier: positive and six corrupt/missing evidence controls')
