from pathlib import Path
log = Path(__file__).with_name('race.log').read_text()
assert '--- PASS: TestRawGraphStartSourceHeaders ' in log
for encoding in ('json', 'protobuf-v1'):
    for retired in ('false', 'true'):
        for parent in ('false', 'true'):
            controls = ['valid', 'transport', 'input-ref-empty', 'input-ref', 'pointer']
            keys = ['Wf-Graph-Start-Token', 'Wf-Input-SHA256']
            parent_keys = ['Wf-Parent-Type', 'Wf-Parent-ID', 'Wf-Parent-Inv-Seq', 'Wf-Parent-Signal']
            if parent == 'true':
                keys += parent_keys
            else:
                for key in parent_keys:
                    controls += [key+'/unexpected-empty', key+'/unexpected-alias']
            for key in keys:
                controls += [key+'/'+mode for mode in ('duplicate', 'conflicting', 'empty-list', 'alias', 'missing')]
            for control in controls:
                assert f'--- PASS: TestRawGraphStartSourceHeaders/{encoding}/retired={retired}/parent={parent}/{control} ' in log
print('all 232 source header controls passed')
