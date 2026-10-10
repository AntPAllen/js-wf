import datetime,hashlib,json,os,subprocess
from pathlib import Path
base=Path(__file__).resolve().parent
fixture=Path('worker/graph_continuation_sdk_test.go')
command=['go','test','-race','./worker','-run','^TestNativeGraphContinuationFailedChildPromise$','-count=1','-timeout=10m','-v']
sources={str(p):hashlib.sha256(p.read_bytes()).hexdigest() for p in [fixture,Path('journal/graph.go'),Path('worker/graph_journal.go')]}
record=dict(source_hashes=sources,command=command,started=datetime.datetime.now(datetime.timezone.utc).isoformat(),fixture_sha256=hashlib.sha256(fixture.read_bytes()).hexdigest(),env=dict(GOMAXPROCS='4',GOMEMLIMIT='512MiB'))
(base/'observed-fixture.go.txt').write_bytes(fixture.read_bytes())
def save(): (base/'observed-command.json').write_text(json.dumps(record,indent=2)+'\n')
save()
with (base/'observed-race.log').open('w') as log:
 result=subprocess.run(command,env={**os.environ,**record['env']},stdout=log,stderr=subprocess.STDOUT)
record.update(exit=result.returncode,finished=datetime.datetime.now(datetime.timezone.utc).isoformat());save()
assert sources=={name:hashlib.sha256(Path(name).read_bytes()).hexdigest() for name in sources}
print(json.dumps(record))
raise SystemExit(result.returncode)
