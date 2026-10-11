import sys
from pathlib import Path
log=Path(sys.argv[1]).read_text()
assert '--- PASS: TestRawGraphOutcomeFieldPresence ' in log
outcome_presence_controls = ['empty', 'whitespace', 'empty-result', 'empty-error', 'failed', 'limit-entry', 'limit-request', 'missing-generation', 'null-generation']
for field in ('result', 'result_ref', 'result_hash', 'error', 'limit_request', 'limit_entry'):
    outcome_presence_controls.append('null-'+field)
for field in ('kind', 'payload'):
    outcome_presence_controls += ['limit-missing-'+field, 'limit-null-'+field]
for encoding in ('json', 'protobuf-v1'):
    for retired in ('false', 'true'):
        for control in outcome_presence_controls:
            assert f'--- PASS: TestRawGraphOutcomeFieldPresence/{encoding}/retired={retired}/{control} ' in log
print('all 76 physical outcome presence cases passed')
