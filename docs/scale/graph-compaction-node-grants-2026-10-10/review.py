"""Review retained development evidence; this is not frozen qualification."""
import gzip
import hashlib
import json
import re
from pathlib import Path

base = Path(__file__).resolve().parent
repo = base.parents[2]
old = [json.loads(line) for line in gzip.open(base / 'prior-compaction-pins.jsonl.gz', 'rt')]
assert len(old) == 14
reads = {'graph_publication_get', 'graph_publication_read_blob'}
pin_results = []
for item in old:
    path = repo / item['path']
    before, after = item['trace'], json.loads(path.read_text())
    def without_reads(trace):
        result = dict(trace)
        result['transport'] = [event for event in trace['transport'] if event['operation'] not in reads]
        return result
    assert without_reads(before) == without_reads(after), item['path']
    pin_results.append({'path': item['path'], 'sha256': hashlib.sha256(path.read_bytes()).hexdigest()})

negative = (base / 'original-node-grant-failures.log').read_text()
positive = (base / 'compaction-controls-race.log').read_text()
prefix = 'TestGraphPrefixCompactionRejectsRevokedNodeGrants/'
leaves = {prefix + f'stream={stream}/height={height}/{fault}'
          for stream in ('', 'archive') for height in (0, 1)
          for fault in ('missing', 'destination', 'location', 'closed')}
def terminals(text, action):
    return set(re.findall(r'--- ' + action + r': (\S+)', text)) & leaves
assert terminals(negative, 'FAIL') == leaves
assert terminals(positive, 'PASS') == leaves
assert not terminals(positive, 'FAIL')
assert 'ok  \tjs-wf/internal/graphpublication\t28.076s' in positive
with gzip.open(base / 'all-853-pins.log.gz', 'rt') as handle:
    pins = handle.read()
assert len(re.findall(r'--- PASS: TestPinnedRegressionCorpus/', pins)) == 853
assert 'ok  \tjs-wf/sim\t15.177s' in pins
native = (base / 'native64-race.log').read_text()
assert 'budget=64 entries=64 archive=true checkpoints=2 effects=0 rejected_request=must_not_run terminal_slot=63' in native
assert 'ok  \tjs-wf/worker\t57.058s' in native
cost = (base / 'cost-race.log').read_text()
assert 'ok  \tjs-wf/journal\t52.643s' in cost
result = dict(scope='Unfrozen development evidence only; no original actual-cap, deployment or collection gate acceptance.',
              original_invalid_publications=16, corrected_rejections=16, saved_pins=853,
              refreshed_compaction_pins=pin_results, all_nonread_trace_fields_unchanged=True,
              bounded_native=dict(budget=64, checkpoints=2, terminal_slot=63, effects=0),
              corrected_compaction_gets=[1093, 12104, 58442])
(base / 'development-review.json').write_text(json.dumps(result, indent=2) + '\n')
print(json.dumps({key: value for key, value in result.items() if key != 'refreshed_compaction_pins'}, indent=2))
