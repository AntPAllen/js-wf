from pathlib import Path
import json,hashlib,subprocess,shutil,struct,datetime,re,calendar
r=Path('/tmp/js-wf-million-candidate-24h-20261005');out=Path('/tmp/js-wf-million-live-receipts-20261005');out.mkdir()
def sha(p):return hashlib.file_digest(p.open('rb'),'sha256').hexdigest()
report_bytes=(r/'campaign/report.json').read_bytes();rep=json.loads(report_bytes);assert rep['status']=='running' and rep['acknowledged_publishes']==rep['scheduled_count']==1000000 and rep['horizon']=='24h0m0s' and rep['receipt_ledger']=='indexed-sha256-128-v1'
(out/'report-before.json').write_bytes(report_bytes)
shutil.copyfile(r/'campaign/receipts.bin',out/'receipts.bin')
(out/'report-after.json').write_bytes((r/'campaign/report.json').read_bytes())
a=json.loads((r/'actual-program.json').read_text());assert sha(Path(f"/proc/{a['pid']}/exe"))==a['sha256']==sha(r/'wf-timer-volume')
info=subprocess.check_output(['go','version','-m',f"/proc/{a['pid']}/exe"],text=True);assert info.splitlines()[1:]==a['go_build_info'].splitlines()[1:]
servers=json.loads((r/'actual-native-processes.json').read_text());assert len(servers)==3
for s in servers:
 assert sha(Path(f"/proc/{s['pid']}/exe"))==s['sha256']==rep['server_candidate_sha256']
 live_info=subprocess.check_output(['go','version','-m',f"/proc/{s['pid']}/exe"],text=True);assert live_info.splitlines()[1:]==s['go_build_info'].splitlines()[1:]
state=subprocess.check_output(['systemctl','--user','show','js-wf-million-candidate-24h-20261005.service','-p','ActiveState','-p','MainPID','-p','SubState'],text=True);assert 'ActiveState=active' in state and 'SubState=running' in state
(out/'service-state.txt').write_text(state);shutil.copyfile(r/'actual-program.json',out/'actual-program.json');shutil.copyfile(r/'actual-native-processes.json',out/'actual-native-processes.json')
m=re.fullmatch(r'(.*)\.(\d+)Z',rep['FirstDue']);assert m
base=calendar.timegm(datetime.datetime.strptime(m[1],'%Y-%m-%dT%H:%M:%S').timetuple())*10**9+int(m[2].ljust(9,'0'))
horizon=24*3600*10**9;data=(out/'receipts.bin').read_bytes();assert len(data)==1000000*40
seen=set();late=[];indices=[]
for i in range(1000000):
 slot=data[i*40:(i+1)*40]
 if slot==bytes(40):continue
 assert hashlib.sha256(slot[:24]).digest()[:16]==slot[24:],('checksum',i)
 sequence,at,server=struct.unpack('<QQQ',slot[:24]);due=base+horizon*i//999999
 assert sequence>0 and sequence not in seen and at>=due and server>=due,('identity/early',i)
 seen.add(sequence);late.append(at-due);indices.append(i)
assert len(seen)>=rep['unique_received'] and len(seen)<1000000
late.sort();review={'source':rep['revision'],'observed_utc':datetime.datetime.now(datetime.timezone.utc).isoformat(),'original_report_status':'running','report_received_before':rep['unique_received'],'validated_snapshot_receipts':len(seen),'zero_slots':1000000-len(seen),'receipt_snapshot_sha256':sha(out/'receipts.bin'),'all_nonzero_slot_checksums_and_unique_sequences_and_no_early_delivery_verified':True,'snapshot_p99_seconds':late[(99*len(late)+99)//100-1]/1e9,'snapshot_max_seconds':late[-1]/1e9,'first_observed_index':min(indices),'last_observed_index':max(indices),'actual_live_program_pid':a['pid'],'actual_program_sha256':a['sha256'],'actual_live_native_pids':[s['pid'] for s in servers],'actual_candidate_sha256':rep['server_candidate_sha256'],'all_live_program_server_hashes_build_fields_verified':True,'full_publication_observed':True,'snapshot_scope':'Read-only copy during live writes; not atomic across report/ledger; valid slots observed, no independent fsync observation. Original SDK sync-before-ack protocol retained. No original mutation, store reopening or interruption verdict.','terminal':False,'qualifies_million_delivery':False,'qualifies_physical_drain':False,'qualifies_candidate_adoption':False,'qualifies_24h':False}
(out/'review.json').write_text(json.dumps(review,indent=2)+'\n');shutil.copyfile(__file__,out/'executed-review.py');print(json.dumps(review))
