from pathlib import Path
import subprocess,json,shutil,datetime,os
repo=Path('/home/exedev/js-wf');root=Path('/tmp/js-wf-bulk-journal-24h-joined-20261006')
assert not root.exists()
assert not subprocess.check_output(['git','status','--porcelain'],cwd=repo)
head=subprocess.check_output(['git','rev-parse','HEAD'],cwd=repo,text=True).strip()
assert head==subprocess.check_output(['git','ls-remote','origin','refs/heads/main'],cwd=repo,text=True).split()[0]
for path in ('joined-journal-ten-minute','joined-consumer-ten-minute'):
 j=json.loads((repo/'docs/scale/sustained-bulk-final-latency-2026-10-06'/path/'independent-review.json').read_text())
 assert j['execution']['status']=='row_verified' and j['execution']['test_exit_code']==0
# The original producer performs authoritative16GiB+128MiB launch admission.
command=['python3',str(repo/'scripts/run-tier3-soak.py'),'--root',str(root),'--row','journal','--duration','24h','--seed','1','--no-race','--memory-limit','4GiB','--chunked-state-retained-audit','--bulk-final-latency','--compare-bulk-point','--explicit-route-seeds','--retained-audit-trace','--audit-wait-stack','--additional-disk-reserve-bytes',str(128*1024**2)]
print(json.dumps(dict(observed_utc=datetime.datetime.now(datetime.timezone.utc).isoformat(),source=head,command=command,free_bytes=shutil.disk_usage('/tmp').free,scope='Fresh original24h journal component only; current full matrices/all-row24h qualification remain separate. Unchanged20s/60s audit,6m final and24h20m SDK deadline. Additional128MiB reserve covers observed small remaining normal-qualifier archival growth; no second native/million campaign.'),indent=2),flush=True)
os.chdir(repo)
os.execvp(command[0],command)
