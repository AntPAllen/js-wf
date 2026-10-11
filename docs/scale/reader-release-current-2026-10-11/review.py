"""Review original controls, migrated pins, actual SDK exits and native closure."""
import hashlib
import json
from pathlib import Path
import re
import subprocess

out = Path(__file__).resolve().parent
repo = out.parents[2]
old_revision = '7eef1c0'
sha = lambda data: hashlib.sha256(data).hexdigest()
inputs = json.loads((out / 'source-inputs.json').read_text())
for name, digest in inputs.items():
    assert sha((repo / name).read_bytes()) == digest, name
old_names = subprocess.check_output(['git', 'ls-tree', '-r', '--name-only', old_revision], text=True).splitlines()
pins = {n for n in old_names if n.startswith('sim/testdata/regressions/') and n.endswith('.json')}
assert pins == {str(p.relative_to(repo)) for p in (repo / 'sim/testdata/regressions').glob('*.json')} and len(pins) == 946
changed = {r['Path']: r for r in json.loads((out / 'migration.json').read_text())}
assert len(changed) == 380 and sum(r['ActorScheduleChanged'] for r in changed.values()) == 6

def controls(trace):
    return [d for d in trace['decisions'] if not any(':' in x for x in d['enabled'])]

for name in sorted(pins):
    original = subprocess.check_output(['git', 'show', old_revision + ':' + name])
    current = (repo / name).read_bytes()
    old, new = json.loads(original), json.loads(current)
    key = name.removeprefix('sim/')
    if key not in changed:
        assert original == current
        continue
    row = changed[key]
    assert sha(original) == row['BeforeSHA256'] and sha(current) == row['AfterSHA256']
    assert {k: v for k, v in old.items() if k not in ('decisions', 'transport')} == {
        k: v for k, v in new.items() if k not in ('decisions', 'transport')}
    assert controls(old) == controls(new)
    if row['ActorScheduleChanged']:
        assert old['workload'] in ('graph_signal_operation_actors', 'graph_signal_caller_retry')
    else:
        assert old['decisions'] == new['decisions']
        i, removed, catalog = 0, 0, 0
        for j, event in enumerate(old['transport']):
            if i < len(new['transport']) and event == new['transport'][i]:
                i += 1
                continue
            if i < len(new['transport']):
                b = new['transport'][i]
                if (event['operation'] == b['operation'] and event['operation'] in
                    ('graph_publication_catalog_high_water', 'graph_publication_next_root') and
                    all(event.get(k) == b.get(k) for k in ('at_ms', 'subject', 'outcome')) and
                    event.get('sequence', 0) - b.get('sequence', 0) == removed and
                    0 <= event.get('expected', 0) - b.get('expected', 0) <= removed):
                    catalog += 1
                    i += 1
                    continue
            assert event['operation'] == 'graph_publication_read_root' and j and event == old['transport'][j-1]
            removed += 1
        assert i == len(new['transport']) and removed == row['Removed'] and catalog == row['CatalogCounters']

# Existing workload runners and all their assertions were not edited.
for name in old_names:
    if name.startswith('sim/') and name.endswith('.go'):
        assert (repo / name).read_bytes() == subprocess.check_output(['git', 'show', old_revision + ':' + name]), name

executables, rows = [], []
for label in ('components-normal', 'components-race', 'native-active10-normal', 'native-active10-race'):
    receipt = json.loads((out / (label + '-command.json')).read_text())
    expected_exit = 1 if label == 'native-active10-race' else 0
    assert receipt['actual_exit'] == expected_exit and receipt['source_unchanged']
    expected = 1 if label.startswith('native') else 2
    assert len(receipt['executions']) == expected
    for execution in receipt['executions'].values():
        assert execution['actual_exit'] == expected_exit
        assert receipt['start_ns'] <= execution['start_ns'] < execution['end_ns'] <= receipt['end_ns']
        assert not Path('/proc/' + str(execution['pid'])).exists()
        path = Path(execution['retained_binary'])
        assert sha(path.read_bytes()) == execution['sha256'] and path.stat().st_size == execution['bytes']
        info = subprocess.check_output(['go', 'version', '-m', str(path)], text=True)
        assert ('-race=true' in info) == label.endswith('race')
        executables.append(execution['sha256'])
    log = (out / (label + '.log')).read_text()
    assert not re.search(r'^\s*--- SKIP:', log, re.M)
    if expected_exit == 0:
        assert not re.search(r'^\s*--- FAIL:', log, re.M)
    if label.startswith('components'):
        assert len(re.findall(r'^\s*--- PASS: TestPinnedRegressionCorpus/[^ ]+ ', log, re.M)) == 946
        for top, cases in (
            ('TestGraphReleaseReaderCurrent', ('valid', 'observe_deadline', 'cas_drop', 'cas_commit_lost', 'cas_commit_unknown', 'overtaken_commit', 'conflict', 'forged', 'revoked')),
            ('TestGraphViewCloseSingleAuthorityObservation', ('valid', 'observe_conflict', 'cas_conflict', 'cas_drop', 'commit_lost', 'commit_unknown', 'overtaken_commit')),
            ('TestGraphStartReadErrorPhases', ('valid', 'open', 'input', 'release', 'release_committed', 'release_committed_unknown', 'input_and_release'))):
            for mode in cases:
                assert f'--- PASS: {top}/{mode} ' in log
    else:
        assert 'count=10 reuse=1 active=true full_scale=false' in log
        assert 'kept 10 other handlers executing before purge' in log
        if expected_exit:
            assert 'signal publish outcome unknown: journal append outcome unknown: context deadline exceeded' in log
            assert '--- FAIL: TestCanonicalGraphTenThousandConcurrentPurgesAndThousandReusedIDs ' in log
            assert 'release start reader:' not in log and 'canonical raw purge/reuse audit=' not in log
        else:
            report = json.loads(re.search(r'canonical raw purge/reuse audit=(\{[^\n]+\})', log)[1])
            assert all(report[k] == 11 for k in ('Invocations', 'Journals', 'Terminal'))
            assert report['Entries'] == 66 and report['Retired'] == report['RetiredTombstones'] == 9
            assert report['References']['ReaderPins'] == 0
        native = Path(receipt['native_root'])
        actual = {str(p.relative_to(native)): dict(bytes=p.stat().st_size, sha256=sha(p.read_bytes()))
                  for p in native.rglob('*') if p.is_file()}
        assert actual == json.loads((out / (label + '-census.json')).read_text())
    rows.append(dict(label=label, actual_exit=expected_exit,
                     named_pass=len(re.findall(r'^\s*--- PASS:', log, re.M))))
result = dict(verified=True, source_inputs=len(inputs), actual_sdk_executions=len(executables), rows=rows,
              original_pins_preserved=946, migrated_pins=380, actor_schedules_refreshed=6,
              catalog_counter_traces=56, redundant_observations_removed=2369,
              family_inventory=163, full_scale_accepted=False, native_failure_cause_confirmed=False)
(out / 'review.json').write_text(json.dumps(result, indent=2) + '\n')
print(json.dumps(result))
