"""Check retained candidate logs and byte equality of the current scan helper."""
import argparse
import hashlib
import json
from pathlib import Path
import re

p = argparse.ArgumentParser(description=__doc__)
p.add_argument('--repo', type=Path, required=True)
p.add_argument('--output', type=Path, required=True)
a = p.parse_args()
root = Path(__file__).resolve().parent
executed = (root / 'executed-native-source.go.txt').read_text()
current = (a.repo / 'integrity/batch_scan_candidate_test.go').read_text()
def helper(source):
    return source[source.index('func candidateBatchScan('):source.index('func candidateDigest(')]
assert helper(executed) == helper(current)
log = (root / 'native-comparison.log').read_text()
assert '--- PASS: TestAuditBatchScanCandidateNativeHighWaterHolesAndReadback' in log
assert '\nFAIL' not in log
match = re.search(r'records=(\d+) point=([\d.]+)s batch=([\d.]+)s digest=([a-f0-9]{64}) consumers=(\d+)', log)
assert match
count, point, batch, digest, consumers = match.groups()
assert int(count) == 34993 and int(consumers) == 0 and float(point) < 20 and float(batch) < 20
controls = (root / 'controls.log').read_text()
assert '--- PASS: TestAuditBatchScanCandidateResolvesConsumerOmissionsAndTail' in controls
assert '--- PASS: TestAuditBatchScanCandidateRejectsOrderSourceAndSemanticFailures' in controls
for case in ['duplicate', 'wrong_stream', 'gap_semantic_error', 'wrong_gap_sequence', 'batch_semantic_error', 'visitor_error']:
    assert '--- PASS: TestAuditBatchScanCandidateRejectsOrderSourceAndSemanticFailures/' + case in controls
assert '--- SKIP: TestAuditBatchScanCandidateNativeHighWaterHolesAndReadback' in controls
result = dict(records=int(count), point_seconds=float(point), batch_seconds=float(batch), ordered_digest=digest, consumer_count_after_scan=int(consumers), reader_helper_matches_executed_bytes=True, reader_helper_sha256=hashlib.sha256(helper(current).encode()).hexdigest(), native_executed_source_sha256=hashlib.sha256(executed.encode()).hexdigest(), native_log_sha256=hashlib.sha256(log.encode()).hexdigest(), controls_log_sha256=hashlib.sha256(controls.encode()).hexdigest(), final_native_test_guard_added_after_comparison=True, actual_successful_native_sdk_retained=False, original_native_stores_retained=False, production_reader_changed=False, qualifies_fault_matrix=False, qualifies_24h_row=False)
a.output.write_text(json.dumps(result, indent=2) + '\n')
print(json.dumps(result, indent=2))
