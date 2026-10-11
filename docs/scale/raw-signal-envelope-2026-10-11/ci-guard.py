import sys
from pathlib import Path
log=Path(sys.argv[1]).read_text()
assert '--- PASS: TestRawGraphSignalEnvelope ' in log
for encoding in ('json', 'protobuf-v1'):
    for stream in ('signal-input', 'signal-queue'):
        levels = {'root': ('input' if stream == 'signal-input' else 'binding', 'index_packet'), 'input': ('schema', 'request', 'index', 'token', 'input_sha256', 'input_size'), 'request': ('type', 'id', 'invocation', 'name', 'key')}
        if stream == 'signal-queue':
            levels['binding'] = ('schema', 'input', 'index', 'sequence')
        for level, fields in levels.items():
            controls = ['valid', 'whitespace', 'unknown', 'alias', 'duplicate']
            for field in fields:
                controls += ['missing-'+field, 'null-'+field]
            for control in controls:
                assert f'--- PASS: TestRawGraphSignalEnvelope/{encoding}/{stream}/{level}/{control} ' in log
print('all 190 signal envelope controls passed')
