import os,json,hashlib,subprocess,time,shutil
from pathlib import Path
repo=Path('/home/exedev/js-wf');base=repo/'docs/scale/graph-public-sdk-2026-10-08';assert not base.exists();base.mkdir()
revision=subprocess.check_output(['git','rev-parse','HEAD'],cwd=repo,text=True).strip()
names=[n for n in subprocess.check_output(['git','ls-files'],cwd=repo,text=True).splitlines() if n.endswith(('.go','.yml')) or n in ('go.mod','go.sum') or n.startswith('sim/testdata/')]
def inventory():return {n:hashlib.sha256((repo/n).read_bytes()).hexdigest() for n in names}
before=inventory()
for n,h in before.items():assert hashlib.sha256(subprocess.check_output(['git','cat-file','blob',revision+':'+n],cwd=repo)).hexdigest()==h,n
(base/'source-before.json').write_text(json.dumps(dict(revision=revision,files=before,selected_inputs_match_git=True),indent=2)+'\n');shutil.copyfile(__file__,base/'executed-regression.py')
consumer=base/'external-consumer';consumer.mkdir()
(consumer/'go.mod').write_text('module graph-sdk-consumer\n\ngo 1.27.1\n\nrequire js-wf v0.0.0\n\nreplace js-wf => /home/exedev/js-wf\n')
(consumer/'consumer.go').write_text('''package graphconsumer
import (
 "context"
 "github.com/nats-io/nats.go/jetstream"
 "js-wf/client"
 "js-wf/journal"
 "js-wf/worker"
)
func Open(ctx context.Context, js jetstream.JetStream, handlers map[string]worker.Handler) (*worker.Worker,*client.Client,error) {
 cfg := journal.NativeGraphConfig{AuthorityStream:"WF_GRAPH_AUTH", AuthorityPrefix:"wf.graph.runtime", ObjectBucket:"WF_GRAPH_OBJECTS",ExpectedReplicas:3}
 configs,err := journal.NativeGraphStreamConfigs(cfg,3)
 if err != nil {return nil,nil,err}
 _ = configs // Provision separately, never during runtime admission.
 store,err := journal.OpenNativeGraphStore(ctx,js,cfg)
 if err != nil {return nil,nil,err}
 w,err := worker.New(ctx,js,"public-worker",handlers,worker.WithGraphJournal(store))
 if err != nil {return nil,nil,err}
 c,err := client.NewWithGraphJournal(js,store)
 return w,c,err
}
func Reuse(ctx context.Context, store *journal.GraphStore, link journal.GraphPayloadLink, expected uint64) (uint64,error) {
 return store.Append(ctx,"flow","id",1,journal.Entry{Kind:journal.Completed,Index:1},expected,nil,[]journal.GraphOwnedPayload{{Index:0,Link:link}})
}
''')
env=dict(os.environ,GOMAXPROCS='2',GOMEMLIMIT='512MiB')
specs=[]
for mode in ('normal','race'):
 flags=['-race'] if mode=='race' else []
 for label,package,pattern in (('journal','./journal','^(TestGraphJournal|TestNativeGraphJournal)'),('worker','./worker','^TestNativeGraphWorkerReplayInputsSignalsAndResults$')):
  specs.append((label+'-'+mode,['go','test','-p=1',*flags,package,'-run',pattern,'-count=1','-timeout=5m','-json'],repo))
specs.append(('external-consumer',['go','test','-p=1','-mod=mod','./...','-count=1','-timeout=5m','-json'],consumer))
results=[]
try:
 for name,command,cwd in specs:
  print('START '+name,flush=True);started=time.monotonic()
  with (base/(name+'.jsonl')).open('w') as out,(base/(name+'.stderr')).open('w') as err:result=subprocess.run(command,cwd=cwd,env=env,stdout=out,stderr=err)
  rows=[json.loads(l) for l in (base/(name+'.jsonl')).read_text().splitlines()]
  ends=[dict(package=r['Package'],action=r['Action'],elapsed=r.get('Elapsed')) for r in rows if r.get('Action') in ('pass','fail','skip') and 'Test' not in r]
  tops=[r['Test'] for r in rows if r.get('Action')=='pass' and r.get('Test') and '/' not in r['Test']]
  item=dict(name=name,command=command,working_directory=str(cwd),environment={k:env[k] for k in ('GOMAXPROCS','GOMEMLIMIT')},exit_code=result.returncode,wall_seconds=time.monotonic()-started,package_results=ends,top_level_passes=tops)
  results.append(item);(base/'results.json').write_text(json.dumps(dict(source=revision,runs=results),indent=2)+'\n')
  print('END '+name+' '+json.dumps(item),flush=True)
  assert result.returncode==0 and ends and not any(r.get('Action')=='fail' for r in rows),item
finally:
 after=inventory();(base/'source-after.json').write_text(json.dumps(dict(revision=revision,files=after,unchanged=before==after),indent=2)+'\n');assert before==after
