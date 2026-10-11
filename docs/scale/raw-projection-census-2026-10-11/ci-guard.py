import sys
from pathlib import Path
log=Path(sys.argv[1]).read_text()
assert '--- PASS: TestRawGraphOrphanProjectionBaseline ' in log
assert '--- PASS: TestRawGraphProjectionCensus ' in log
for encoding in ('json', 'protobuf-v1'):
    for archive in ('false', 'true'):
        for control in ('valid', 'orphan', 'bad-key', 'missing-census', 'reader-differs', 'reader-missing', 'missing-terminal', 'empty-running', 'running-projection', 'retired-survivor', 'retired-survivor-wrong-generation', 'retired-cleared'):
            assert f'--- PASS: TestRawGraphProjectionCensus/{encoding}/archive={archive}/{control} ' in log
for replicas in (1, 3):
    for indexed in ('false', 'true'):
        assert f'RAW_PROJECTION_CENSUS orphan=rejected R{replicas} indexed={indexed}' in log
        assert f'RAW_CHECKPOINT_AUDIT stage=projection-tombstone entries=5 terminals=1 R{replicas} indexed={indexed}' in log
print('all projection census controls and native orphan/tombstone receipts passed')
