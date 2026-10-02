#!/usr/bin/env python3
"""Decode Go vectors with generated Python protobuf, then encode reverse vectors.

Use protoc --python_out=GENERATED_ROOT protocol/v1/journal.proto and pass that
root as argument one. Argument two is a new output directory for Go validation.
"""
import base64
import json
from pathlib import Path
import sys

sys.path.insert(0, sys.argv[1])
from protocol.v1 import journal_pb2

output = Path(sys.argv[2])
output.mkdir(parents=True, exist_ok=False)
vectors = json.loads(Path("protocol/testdata/vectors.json").read_text())
kinds = ["Started", "StepRequested", "StepCompleted", "Suspended", "SignalConsumed", "Attempt", "Completed", "Failed"]
if len(vectors) != len(kinds):
    raise ValueError("missing kind vectors")
reverse = []
for i, vector in enumerate(vectors):
    message = journal_pb2.JournalRecord()
    message.ParseFromString(base64.b64decode(vector["WireBase64"], validate=True))
    expected = (1, int(vector["Sequence"]), int(vector["Epoch"]), int(vector["Index"]), i + 1,
                base64.b64decode(vector["PayloadBase64"], validate=True), vector["WorkerID"])
    observed = (message.version, message.sequence, message.entry.epoch, message.entry.index,
                message.entry.kind, message.entry.payload_json, message.entry.worker_id)
    if observed != expected or vector["Kind"] != kinds[i]:
        raise ValueError(f"Go-to-Python mismatch: {vector['Name']}")
    # Independently create messages and different boundary values/payload bytes.
    payload = b' {"integer":9007199254740993,"null":null} \n'
    entry = journal_pb2.JournalEntry(epoch=(1 << 64) - 1 - i, index=(1 << 53) + 17 + i,
                                   kind=i + 1, payload_json=payload, worker_id="python-λ")
    produced = journal_pb2.JournalRecord(version=1, sequence=(1 << 64) - 100 - i, entry=entry)
    reverse.append(dict(Name=kinds[i], Kind=kinds[i], Sequence=str(produced.sequence),
                        Epoch=str(entry.epoch), Index=str(entry.index), WorkerID=entry.worker_id,
                        PayloadBase64=base64.b64encode(payload).decode(),
                        WireBase64=base64.b64encode(produced.SerializeToString()).decode()))
(output / "vectors.json").write_text(json.dumps(reverse, indent=2) + "\n")
print(f"PASS Go-to-Python: {len(vectors)} records; wrote {len(reverse)} independent Python-to-Go records")
