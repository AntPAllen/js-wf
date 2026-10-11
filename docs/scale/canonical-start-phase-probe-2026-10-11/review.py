"""Bind both closed experiments and independently check each recovery packet."""
import base64
import gzip
import hashlib
import json
from pathlib import Path

out = Path(__file__).resolve().parent
repo = out.parents[2]

def census(directory):
    return {str(p.relative_to(directory)): dict(
        bytes=p.stat().st_size, sha256=hashlib.sha256(p.read_bytes()).hexdigest())
        for p in directory.rglob('*') if p.is_file()}

reviews = []
for directory in (out, out / 'ready'):
    receipt = json.loads((directory / 'run-command.json').read_text())
    root = Path(receipt['command'][1])
    assert not Path('/proc/' + str(receipt['pid'])).exists()
    assert receipt['source_unchanged']
    assert hashlib.sha256((root / 'probe').read_bytes()).hexdigest() == receipt['binary_sha256']
    assert hashlib.sha256((directory / 'probe.go.txt').read_bytes()).hexdigest() == receipt['helper_sha256']
    inputs = json.loads((directory / 'source-inputs.json').read_text())
    for name, sha in inputs.items():
        assert hashlib.sha256((root / 'source' / name).read_bytes()).hexdigest() == sha, name
    marker = json.loads((directory / 'copy-verified.json').read_text())
    original = Path(marker['original'])
    expected = json.loads((original.parent / 'native-census.json').read_text())
    assert census(original) == expected
    assert len(expected) == marker['files'] == 2443
    assert sum(v['bytes'] for v in expected.values()) == marker['bytes'] == 198307813
    copied = census(root / 'store-copy')
    (directory / 'closed-copy-census.json.gz').write_bytes(gzip.compress(
        (json.dumps(copied, indent=2, sort_keys=True) + '\n').encode(), mtime=0))
    result = json.loads((directory / 'result.json').read_text())
    observations = {o['Phase']: o for o in result['Observations']}
    assert len(observations) == len(result['Observations'])
    assert observations['node0.StreamInfo(PURGE_SCALE_AUTH)']['Error'] == 'context deadline exceeded'
    assert all(not o['Error'] for name, o in observations.items()
               if '.StreamInfo(' in name and name != 'node0.StreamInfo(PURGE_SCALE_AUTH)')
    review = dict(actual_exit=receipt['actual_exit'], source_inputs=len(inputs),
                  binary_sha256=receipt['binary_sha256'], original_unchanged=True,
                  copy_files=len(copied), copy_bytes=sum(v['bytes'] for v in copied.values()),
                  first_metadata_deadline=True, full_scale_accepted=False,
                  original_failure_cause_confirmed=False)
    if directory == out:
        assert receipt['actual_exit'] == 1
        assert result['Error'] == 'node0.StreamInfo(PURGE_SCALE_AUTH): context deadline exceeded'
        assert 'OpenNativeGraphStore' not in observations
    else:
        assert receipt['actual_exit'] == 0 and not result['Error']
        for name in ('PURGE_SCALE_AUTH', 'OBJ_PURGE_SCALE_OBJECTS', 'WF_INV', 'WF_RUN'):
            assert not observations['node0.metadataRecheck(' + name + ')']['Error']
        catalog = observations['rawStartCatalog']['Value']
        assert catalog == dict(bound=984, pending=43)
        assert observations['boundDescriptorsUnchanged']['Value'] == 984
        recovered = [o for name, o in observations.items() if name.startswith('pending.RecoverStartAttempt(')]
        assert len(recovered) == 43
        sequences, existing = set(), 0
        for o in recovered:
            value = o['Value']
            after, handle, source = value['after'], value['handle'], value['source']
            descriptor = after['State']['Start']
            typ, identity = descriptor['request']['type'], descriptor['request']['id']
            assert o['Phase'] == 'pending.RecoverStartAttempt(' + identity + ')'
            before = observations['pending.ReadStart(' + identity + ')']['Value']
            assert before['state']['Pending'] and before['state']['Invocation'] == 0
            assert before['state']['Start'] == descriptor
            input_bytes = json.dumps(before['input'], ensure_ascii=False, separators=(',', ':')).encode()
            assert len(input_bytes) == descriptor['input_size']
            assert hashlib.sha256(input_bytes).hexdigest() == descriptor['input_sha256']
            assert not after['State']['Pending']
            assert after['JournalCount'] == after['JournalTail'] == 0
            assert not after['Kind'] and not after['Retired'] and not after['Purging']
            assert handle['type'] == typ == 'purge-scale' and handle['id'] == identity
            seq = handle['inv_seq']
            assert seq > 0 and seq == after['State']['Invocation'] == source['Sequence']
            assert seq not in sequences
            sequences.add(seq)
            prior = observations['pending.sourceBefore(' + identity + ')']['Value']
            if prior:
                existing += 1
                assert prior == seq
            assert source['Subject'] == 'wf.inv.' + typ + '.' + identity
            assert source['Header']['Wf-Graph-Start-Token'] == [descriptor['token']]
            assert source['Header']['Wf-Input-SHA256'] == [descriptor['input_sha256']]
            pointer = json.loads(base64.b64decode(source['Data']))
            assert pointer == dict(schema='js-wf-canonical-start-pointer-v1', token=descriptor['token'])
        assert existing == 6
        review.update(pending_recovered=43, existing_sources_preserved=6,
                      missing_sources_bound=37, bound_descriptors_preserved=984,
                      recovery_verified=True,
                      max_recovery_seconds=max(o['Nanos'] for o in recovered) / 1e9)
    reviews.append(review)
(out / 'review.json').write_text(json.dumps(reviews, indent=2) + '\n')
print(json.dumps(reviews))
