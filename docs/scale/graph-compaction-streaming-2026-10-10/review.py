"""Review directed development evidence, not a frozen qualification."""
import gzip
import hashlib
import json
import re
from pathlib import Path

base = Path(__file__).resolve().parent
repo = base.parents[2]
inputs = json.loads((base / 'source-inputs.json').read_text())
assert {name: hashlib.sha256((repo / name).read_bytes()).hexdigest() for name in inputs} == inputs
node_prefix = 'TestGraphPrefixCompactionRejectsRevokedNodeGrants/'
node_cases = {node_prefix + f'stream={stream}/height={height}/{fault}'
              for stream in ('', 'archive') for height in (0, 1)
              for fault in ('missing', 'destination', 'location', 'height', 'stream', 'kind', 'closed')}
record_prefix = 'TestGraphPrefixCompactionRejectsChangedRelocationRecords/'
record_cases = {record_prefix + f'stream={stream}/{fault}' for stream in ('', 'archive') for fault in ('data', 'payload')}
def terminals(raw, action, cases):
    return set(re.findall(r'--- ' + action + r': (\S+)', raw)) & cases
nodes = (base / 'compaction-controls-race.log').read_text()
records = (base / 'record-controls-race.log').read_text()
assert terminals(nodes, 'PASS', node_cases) == node_cases
assert terminals(records, 'PASS', record_cases) == record_cases
assert '--- FAIL:' not in nodes + records
assert terminals((base / 'node-grant-bypass.log').read_text(), 'FAIL', node_cases) == node_cases
assert terminals((base / 'record-comparison-bypass.log').read_text(), 'FAIL', record_cases) == record_cases
iterator = (base / 'iterator-race.log').read_text()
assert '--- PASS: TestRangeIteratorStreamsFreshNodesAndFailsClosed' in iterator
assert '--- PASS: TestReadRangeOrderedBoundedTraversalAndFailures' in iterator
assert 'ok  \tjs-wf/internal/retainedgraph\t1.612s' in iterator
cost = (base / 'cost-race.log').read_text()
gets = {int(entries): int(count) for entries, count in re.findall(r'entries=(\d+).*compaction_gets=(\d+)', cost)}
assert gets == {36: 569, 260: 4924, 1028: 21566}
assert 'virtual_elapsed=57s renewals=28' in cost
assert 'ok  \tjs-wf/journal\t39.098s' in cost
old_pins = [json.loads(line) for line in gzip.open(base / 'prior-compaction-pins.jsonl.gz', 'rt')]
assert len(old_pins) == 14
for item in old_pins:
    before, after = item['trace'], json.loads((repo / item['path']).read_text())
    def without_gets(trace):
        out = dict(trace)
        out['transport'] = [event for event in trace['transport'] if event['operation'] != 'graph_publication_get']
        return out
    assert without_gets(before) == without_gets(after), item['path']
with gzip.open(base / 'all-853-pins.log.gz', 'rt') as file:
    pins = file.read()
assert len(re.findall(r'--- PASS: TestPinnedRegressionCorpus/', pins)) == 853
assert 'ok  \tjs-wf/sim\t15.620s' in pins
native = (base / 'native64-recovery-race.log').read_text()
assert 'budget=64 entries=64 archive=true checkpoints=2 effects=0 rejected_request=must_not_run terminal_slot=63' in native
assert 'original_receipts_physically_reclaimed=19 before_stage=true' in native
assert 'ok  \tjs-wf/worker\t58.580s' in native
result = dict(scope='Unfrozen development checks only; actual100000, complete extended/native/fault/deployment gates remain open.',
              source_sha256=inputs, node_grant_rejections=28, record_equality_rejections=4,
              required_negative_failures=dict(node_grant_bypass=28, record_comparison_bypass=4),
              compaction_gets=gets, safe_previous_compaction_gets={36:1093,260:12104,1028:58442},
              saved_pins=853, refreshed_compaction_pins=14, all_non_Get_trace_fields_unchanged=True,
              bounded_native=dict(budget=64, checkpoints=2, terminal_slot=63, effects=0),
              handoff_recovery_original_receipts_physically_reclaimed=19)
(base / 'development-review.json').write_text(json.dumps(result, indent=2) + '\n')
print(json.dumps({key: value for key, value in result.items() if key != 'source_sha256'}, indent=2))
