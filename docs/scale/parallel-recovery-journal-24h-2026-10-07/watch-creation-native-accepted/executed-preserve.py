from storage_review_common import *
root=Path('/tmp/js-wf-state-watch-creation-native-v2-20261007');out=repo/'docs/scale/parallel-recovery-journal-24h-2026-10-07/watch-creation-native-accepted';out.mkdir()
before=closure(root)
for name in ['execution.json','actual-sdk.json','binary.json','commands.json','native.log','build.log','source-before.json','source-after.json','external-source-before.json','external-source-after.json','closure.json','executed-producer.py']:
 shutil.copyfile(root/name,out/name)
for name in ['archive-verification.json','fixture-inventory.json']:
 shutil.copyfile(Path(str(root)+'-proof')/name,out/name)
fixture=out/'fixture';fixture.mkdir()
for name in ['native-proof.json','expected-values.json','wire.jsonl']:
 shutil.copyfile(root/'fixture'/name,fixture/name)
shutil.copyfile('/tmp/js-wf-state-watch-creation-native-v2-independent-review-20261007.json',out/'independent-review.json')
shutil.copyfile('/tmp/js-wf-state-watch-creation-proof-controls-20261007.json',out/'proof-controls.json')
for name in ['review-state-creation-native.py','check-state-creation-proof-controls.py','worker_leaf_wire.py','check-tier2-journal-shard.py']:
 shutil.copyfile(repo/'scripts'/name,out/('executed-'+name))
shutil.copyfile(__file__,out/'executed-preserve.py');shutil.copyfile('/tmp/storage_review_common.py',out/'executed-common.py')
assert fixture_archive.inventory(root)==json.loads((out/'fixture-inventory.json').read_text())['files']
(out/'full-closure.json').write_text(json.dumps(dict(before=before,after=closure(root)),indent=2)+'\n')
failed=repo/'docs/scale/parallel-recovery-journal-24h-2026-10-07/watch-creation-native-initial-setup-failure'
shutil.copyfile('/tmp/js-wf-state-watch-creation-initial-failed-s3-20261007.json',failed/'s3-readback.json')
print('STATE_CREATION_NATIVE_COMPLETE_ACCEPTED_FIXTURE_PRESERVED',flush=True)
