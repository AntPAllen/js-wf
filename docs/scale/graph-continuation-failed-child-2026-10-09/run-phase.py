import datetime,hashlib,json,os,subprocess
from pathlib import Path
base=Path(__file__).resolve().parent
fixture=Path('worker/graph_continuation_sdk_test.go')
command=['go','test','-race','./worker','-run','^TestNativeGraphContinuationFailedChildPromise/R3Domain/archive=true$','-count=1','-timeout=3m','-v']
record=dict(command=command,started=datetime.datetime.now(datetime.timezone.utc).isoformat(),fixture_sha256=hashlib.sha256(fixture.read_bytes()).hexdigest(),env=dict(GOMAXPROCS='4',GOMEMLIMIT='512MiB'))
(base/'phase-fixture.go.txt').write_bytes(fixture.read_bytes())
def save(): (base/'phase-command.json').write_text(json.dumps(record,indent=2)+'\n')
save()
with (base/'phase-race.log').open('w') as log:
 result=subprocess.run(command,env={**os.environ,**record['env']},stdout=log,stderr=subprocess.STDOUT)
record.update(exit=result.returncode,finished=datetime.datetime.now(datetime.timezone.utc).isoformat());save()
assert record['fixture_sha256']==hashlib.sha256(fixture.read_bytes()).hexdigest()
print(json.dumps(record))
raise SystemExit(result.returncode)
