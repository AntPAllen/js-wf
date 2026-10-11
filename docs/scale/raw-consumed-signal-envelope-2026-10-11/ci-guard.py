import sys
from pathlib import Path
log=Path(sys.argv[1]).read_text()
assert '--- PASS: TestRawGraphConsumedSignalEnvelope ' in log
for encoding in ('json', 'protobuf-v1'):
    for level in ('root', 'canonical'):
        required = ('sig_seq', 'name') if level == 'root' else ('index', 'token')
        nullable = ('sig_seq', 'name', 'payload', 'ref', 'hash', 'graph_child', 'canonical_signal') if level == 'root' else required
        controls = ['valid', 'whitespace', 'unknown', 'alias', 'duplicate', 'escaped-duplicate']
        controls += ['missing-'+field for field in required]
        controls += ['null-'+field for field in nullable]
        for control in controls:
            assert f'--- PASS: TestRawGraphConsumedSignalEnvelope/{encoding}/{level}/{control} ' in log
print('all 50 consumed signal envelope controls passed')
