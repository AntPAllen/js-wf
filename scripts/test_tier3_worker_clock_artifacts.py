import base64
import copy
import hashlib
import importlib.util
import json
from pathlib import Path
import tempfile
import unittest

spec=importlib.util.spec_from_file_location('rows_clock',Path(__file__).with_name('check-tier3-journal-row.py'))
r=importlib.util.module_from_spec(spec);spec.loader.exec_module(r)
c=r.load('clock_artifact_helper','tier3-worker-clock-evidence.py')


class WorkerClockArtifacts(unittest.TestCase):
    def write(self,root,name,value):
        path=root/name;path.parent.mkdir(parents=True,exist_ok=True)
        path.write_text(json.dumps(value))

    def fixture(self,root):
        stamp=lambda s:f'2026-10-03T12:00:{s:02d}.123456789Z'
        steps=[];fences=[];sessions=[];binaries=[]
        for slot,(worker,offset) in enumerate(c.OFFSETS.items()):
            step=dict(Worker=worker,At=c.corrected_timestamp(stamp(1),-offset),Stage='fetched',RunSequence=7,Delivery=2)
            fence=dict(step,At=c.corrected_timestamp(stamp(2),-offset),Epoch=7,Reason='lease_cleanup_lost',Error='lost')
            steps.append(step);fences.append(fence)
            (root/(worker+'-dispatch.jsonl')).write_text(json.dumps(step)+'\n')
            (root/(worker+'-fencing.jsonl')).write_text(json.dumps(dict(pid=100+slot,sequence=1,event=fence))+'\n')
            metrics=dict(fencing_events=1)
            self.write(root,worker+'-metrics.json',dict(pid=100+slot,worker_id=worker,metrics=metrics))
            sessions.append(dict(worker_id=worker,pid=100+slot,generation=0,exit_success=True,exit_signal=0,final_metrics=metrics,dispatch_records=1,fencing_records=1,partial_dispatch_tail=False,partial_fencing_tail=False))
            name=f'binary-{slot%3}.test';data=f'synthetic guard binary {slot%3}'.encode()
            (root/name).write_bytes(data)
            binaries.append(dict(worker=worker,offset_ns=offset,path=name,sha256=hashlib.sha256(data).hexdigest()))
        self.write(root,'process-evidence.json',sessions)
        self.write(root,'worker-clock-binaries.json',binaries)
        for name,records in [('dispatch',steps),('fencing',fences)]:
            self.write(root,name+'-worker-clock-raw.json',records)
            self.write(root,name+'.json',c.normalize_records(records))
        proofs=[]
        for cut,(stage,second) in enumerate([('initial',0),('periodic',30),('final',35)]):
            samples=[];messages=[]
            for slot,(worker,offset) in enumerate(c.OFFSETS.items()):
                at=c.corrected_timestamp(stamp(second),-offset);sequence=10*cut+slot+1
                sample=dict(worker=worker,worker_at=at,server_at=stamp(second),sequence=sequence,offset_ns=offset)
                samples.append(sample)
                payload=dict(worker=worker,worker_at=at,sequence=0,offset_ns=0)
                messages.append(dict(subject='matrix.clock.'+worker,sequence=sequence,time=stamp(second),data=base64.b64encode(json.dumps(payload).encode()).decode()))
            proofs.append(dict(stage=stage,observed=stamp(second),samples=samples,messages=messages,stream_info=dict(config=dict(name='MATRIX_CLOCK',subjects=['matrix.clock.*'],num_replicas=5,storage='file',max_msgs_per_subject=16),cluster=dict(leader='n0',replicas=[dict(name=f'n{i}',current=True) for i in range(1,5)]))))
        self.write(root,'worker-clock-proofs.json',proofs)
        self.write(root,'faults.json',[dict(node=-1,scheduled=stamp(30),killed=stamp(30),healed=stamp(30),clock_samples=proofs[1]['samples'])])
        original='\tsec, nsec, mono := runtimeNow()\n';(root/'clock-original-time.go').write_text(original)
        for i,seconds in [(0,5),(1,-5)]:
            directory=root/f'clock-{i}';directory.mkdir()
            (directory/'skew-time.go').write_text(original+f'\tsec += {seconds} // test-only wall-clock skew\n')
            self.write(root,f'clock-{i}/skew-overlay.json',dict(Replace={'/go/src/time/time.go':f'/tmp/clock-{i}/skew-time.go'}))
        source=dict(revision='a'*40,clean=True,files={'go.mod':'b'*64})
        for stage in ('before','after'):self.write(root,f'worker-clock-source-{stage}.json',source)
        return dict(confirmed_faults=1,duration_seconds=35)

    def test_complete_guard_and_precise_rejections(self):
        for mutation in (None,'raw_changed','normalized_changed','counter_changed','wrong_offset','binary_changed','same_binary','missing_probe','fault_changed','source_changed','overlay_changed','stale_replica'):
            with self.subTest(mutation=mutation),tempfile.TemporaryDirectory() as directory:
                root=Path(directory);report=self.fixture(root)
                def change(name,fn):
                    data=json.loads((root/name).read_text());fn(data);self.write(root,name,data)
                if mutation=='raw_changed':change('dispatch-worker-clock-raw.json',lambda x:x[0].update(Delivery=99))
                elif mutation=='normalized_changed':change('fencing.json',lambda x:x[0].update(Epoch=99))
                elif mutation=='counter_changed':change('process-evidence.json',lambda x:x[0]['final_metrics'].update(fencing_events=0))
                elif mutation=='wrong_offset':change('worker-clock-binaries.json',lambda x:x[0].update(offset_ns=0))
                elif mutation=='binary_changed':(root/'binary-0.test').write_bytes(b'changed')
                elif mutation=='same_binary':change('worker-clock-binaries.json',lambda x:x[1].update(path=x[0]['path'],sha256=x[0]['sha256']))
                elif mutation=='missing_probe':change('worker-clock-proofs.json',lambda x:x.pop())
                elif mutation=='fault_changed':change('faults.json',lambda x:x[0].update(healed='2026-10-03T12:00:31Z'))
                elif mutation=='source_changed':change('worker-clock-source-after.json',lambda x:x.update(revision='c'*40))
                elif mutation=='overlay_changed':(root/'clock-0/skew-time.go').write_text('no actual overlay')
                elif mutation=='stale_replica':change('worker-clock-proofs.json',lambda x:x[0]['stream_info']['cluster']['replicas'][0].update(current=False))
                if mutation is None:
                    result=r.check_worker_clock_artifacts(root,report)
                    self.assertEqual(result['broker_clock_messages'],15)
                    self.assertEqual(result['graceful_counter_cross_checks'],5)
                    self.assertFalse(result['clears_full_tier3_release'])
                else:
                    with self.assertRaises(ValueError):r.check_worker_clock_artifacts(root,report)
