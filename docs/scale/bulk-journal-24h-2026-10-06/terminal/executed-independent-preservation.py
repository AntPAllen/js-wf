from pathlib import Path
import sys,json,hashlib,subprocess,importlib.util
sys.dont_write_bytecode=True
repo=Path('/home/exedev/js-wf');sys.path.insert(0,str(repo/'scripts'));import fixture_archive
root=Path('/tmp/js-wf-bulk-journal-24h-joined-20261006');out=repo/'docs/scale/bulk-journal-24h-2026-10-06/terminal';archive=Path('/tmp/js-wf-bulk-journal-24h-joined-complete-20261006.tar.gz')
meta=json.loads((out/'archive-verification.json').read_text());inventory=json.loads((out/'fixture-inventory.json').read_text());execution=json.loads((root/'execution.json').read_text())
assert execution['status']=='failed' and execution['test_exit_code']==1 and execution['duration']=='24h' and execution['source']=='757454ab9681f044af469a9462ecb7dfc63233ad'
assert json.loads((root/'source-before.json').read_text())==json.loads((root/'source-after.json').read_text())
assert fixture_archive.verify(archive)==inventory and fixture_archive.inventory(root)==inventory['files']
with archive.open('rb') as stream:assert fixture_archive.digest(stream)==dict(bytes=meta['archive_bytes'],sha256=meta['archive_sha256'])
assert hashlib.sha256((out/'fixture-inventory.json').read_bytes()).hexdigest()==meta['inventory_sha256']
events=[json.loads(line) for line in (root/'events.jsonl').read_text().splitlines()];native=''.join(e.get('Output','') for e in events)
assert 'intermediate retained audit batch=8520 cutoff=238560' in native and '--- FAIL: TestFiveContainerMixedJournalLeaderEveryThirtySeconds (61168.65s)' in native
spec=importlib.util.spec_from_file_location('shared',repo/'scripts/run-domain-runtime-controls.py');shared=importlib.util.module_from_spec(spec);spec.loader.exec_module(shared)
closure=shared.closure(root)
report=dict(source=execution['source'],status='native_failed_preservation_verified',actual_24h_accepted=False,native_body_seconds=61168.65,batch=8520,cutoff_invocations=238560,third_attempt_journals=179520,third_attempt_entries=1438910,attempt_seconds=20,total_audit_deadline_seconds=60,archive=meta,source_before_after_equal=True,complete_archive_and_current_files_equal=True,fresh_visible_closure=closure,stores_reopened=False,scope='Preservation verification only. Terminal original native failure, not observer timeout. Server-side/performance cause remains unconfirmed. No restart, narrowed gate, changed deadline or partial acceptance.')
(out/'independent-preservation-verification.json').write_text(json.dumps(report,indent=2)+'\n')
print(json.dumps({k:v for k,v in report.items() if k not in ('fresh_visible_closure','archive')}))
