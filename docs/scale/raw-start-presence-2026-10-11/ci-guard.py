import sys
from pathlib import Path
log=Path(sys.argv[1]).read_text()
assert '--- PASS: TestRawGraphStartFieldPresence ' in log
start_presence_controls = ['valid', 'whitespace', 'optional-zero']
for field in ('schema', 'token', 'request', 'input_sha256', 'input_size', 'request.type', 'request.id'):
    start_presence_controls += ['missing-'+field, 'null-'+field]
for field in ('parent_type', 'parent_id', 'parent_invocation', 'signal_name'):
    start_presence_controls.append('null-request.'+field)
for encoding in ('json', 'protobuf-v1'):
    for scope in ('cursor-live', 'cursor-retired', 'owned-input'):
        for control in start_presence_controls:
            assert f'--- PASS: TestRawGraphStartFieldPresence/{encoding}/{scope}/{control} ' in log
print('all 126 physical start presence cases passed')
