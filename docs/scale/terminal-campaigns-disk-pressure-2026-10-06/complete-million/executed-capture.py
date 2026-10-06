from pathlib import Path
import sys,json,subprocess,hashlib,importlib.util,shutil,datetime
repo=Path('/home/exedev/js-wf');sys.path.insert(0,str(repo/'scripts'));import fixture_archive
spec=importlib.util.spec_from_file_location('closed',repo/'scripts/verify-tier2-closed-originals.py');closed=importlib.util.module_from_spec(spec);spec.loader.exec_module(closed)
head=subprocess.check_output(['git','rev-parse','HEAD'],cwd=repo,text=True).strip();assert head==subprocess.check_output(['git','ls-remote','origin','refs/heads/main'],cwd=repo,text=True).split()[0]
assert shutil.disk_usage(repo).free>1024**3,'Need 1GiB free for finite archival; no SDK/NATS launch'
items=[('journal',Path('/tmp/js-wf-chunked-journal-24h-20261006'),'js-wf-chunked-journal-24h-20261006.service',[861924,862308,862338]),('million',Path('/tmp/js-wf-million-candidate-24h-20261005'),'js-wf-million-candidate-24h-20261005.service',[101005,101283])]
archives=Path('/tmp/js-wf-terminal-campaign-archives-20261006');archives.mkdir()
for name,root,unit,pids in items:
 state=subprocess.check_output(['systemctl','--user','show',unit,'-p','ActiveState','-p','SubState','-p','Result'],text=True);assert 'ActiveState=failed' in state
 assert all(not Path('/proc',str(pid)).exists() for pid in pids)
 fd=closed.verify_no_open_originals(root)
 out=repo/'docs/scale/terminal-campaigns-disk-pressure-2026-10-06'/('complete-'+name)
 proof=fixture_archive.capture(root,archives/(name+'.tar.gz'),out)
 scope=dict(recorded_main=head,root=str(root),unit=unit,terminal_state=state,known_pids_closed=pids,visible_fd_before_capture=fd,original_fixture_bytes_unchanged=proof['all_current_fixture_files_unchanged_after_capture'],observed_utc=datetime.datetime.now(datetime.timezone.utc).isoformat(),scope='Complete closed failed campaign archival; preserves partial/no-space artifacts exactly. No original file repair, gate promotion or SDK/NATS startup. Canonical full archive intended for S3; Git contains manifests/hash/receipt, no archive-part duplicates.')
 (out/'capture-scope.json').write_text(json.dumps(scope,indent=2)+'\n');shutil.copyfile(__file__,out/'executed-capture.py')
 print('CAPTURED',name,proof['archive_bytes'],proof['archive_sha256'],flush=True)
