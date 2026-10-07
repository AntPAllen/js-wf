from storage_review_common import *
root=Path('/tmp/js-wf-parallel-recovery-journal-24h-20261007');out=repo/'docs/scale/parallel-recovery-journal-24h-2026-10-07/terminal-failure';out.mkdir()
unit='js-wf-parallel-recovery-journal-24h-20261007.service'
properties=subprocess.check_output(['systemctl','show',unit,'-p','ActiveState','-p','SubState','-p','ExecMainPID','-p','ExecMainStatus','-p','Result','-p','InvocationID'],text=True)
assert 'InvocationID=cb8aebddd6d44f2b947fbb9604ada1da\n' in properties and 'ExecMainPID=3743034\n' in properties and 'ExecMainStatus=1\n' in properties and 'Result=exit-code\n' in properties
(out/'original-terminal-unit.txt').write_text(properties)
shutil.copyfile(__file__,out/'executed-preserve.py');shutil.copyfile('/tmp/storage_review_common.py',out/'executed-common.py')
before=closure(root)
for name in ['execution.json','independent-review.json','events.jsonl','watch-watch-result.json','commands.json','source-before.json','source-after.json','binary.json','test-environment.json','executed-review.py']:
 shutil.copyfile(root/name,out/name)
traces=out/'checkpoint4160';traces.mkdir()
for p in (root/'fixture').glob('audit-batch-4160-*'):shutil.copyfile(p,traces/p.name)
proof=fixture_archive.capture(root,Path('/tmp/js-wf-parallel-recovery-journal-24h-failed-complete-20261007.tar.gz'),out,compresslevel=1)
after=closure(root)
(out/'closure.json').write_text(json.dumps(dict(before=before,after=after),indent=2)+'\n')
print('FAILED24H_CAPTURED',proof['archive_bytes'],proof['members'],proof['archive_sha256'],flush=True)
