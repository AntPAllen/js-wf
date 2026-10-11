import sys
from pathlib import Path
log = Path(sys.argv[1]).read_text()
assert '--- PASS: TestRawGraphCursorFieldPresence ' in log
presence_controls = ['valid', 'optional-zero']
for field in ('schema', 'invocation', 'base', 'count', 'epoch', 'kind', 'retired'):
    presence_controls += ['missing-'+field, 'null-'+field]
for field in ('purging', 'start', 'previous_invocation', 'signal_inputs', 'signal_bindings', 'signal_source', 'signal_consumed', 'signal_repair', 'retained_from', 'checkpoint'):
    presence_controls.append('null-'+field)
for encoding in ('json', 'protobuf-v1'):
    for archive in ('false', 'true'):
        for retired in ('false', 'true'):
            for control in presence_controls:
                assert f'--- PASS: TestRawGraphCursorFieldPresence/{encoding}/archive={archive}/retired={retired}/{control} ' in log
print('all 208 physical cursor presence cases passed')
