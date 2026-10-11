import sys
from pathlib import Path
log=Path(sys.argv[1]).read_text()
assert '--- PASS: TestRawGraphChildSignalHistory ' in log
for encoding in ('json', 'protobuf-v1'):
    for control in ('inline', 'failed', 'external', 'no-declaration', 'missing-child', 'wrong-type', 'wrong-id', 'zero-generation', 'outcome-generation', 'child-hash-only', 'failed-result', 'null-error', 'unknown-outcome', 'external-outcome-ref', 'external-unowned', 'external-wrong-bytes', 'mixed-body'):
        assert f'--- PASS: TestRawGraphChildSignalHistory/{encoding}/{control} ' in log
print('all 34 physical child history controls passed')
