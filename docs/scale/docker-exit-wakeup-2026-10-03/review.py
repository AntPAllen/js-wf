#!/usr/bin/env python3
"""Review actual Docker exit tests and exact compiled controls at pinned Git."""
import argparse,hashlib,json,subprocess
from pathlib import Path
p=argparse.ArgumentParser(description=__doc__);p.add_argument('--root',type=Path,required=True);p.add_argument('--output',type=Path,required=True);a=p.parse_args();r=a.root
revision='d2b98c6705bd2f47a680048a212d76608fa4b613'
terminal=json.loads((r/'terminal.json').read_text())
assert terminal['databaseId']==37112424062 and terminal['headSha']==revision and terminal['status']=='completed' and terminal['conclusion']=='success'
source=json.loads((r/'source.json').read_text());assert source['head']==revision and source['image'].startswith('sha256:')
for name,digest in source['files'].items():
 assert hashlib.sha256(subprocess.check_output(['git','show',revision+':'+name])).hexdigest()==digest
original=subprocess.check_output(['git','show',revision+':testcluster/docker_cluster.go']).decode()
begin=original.index('func observeDockerKill(');end=original.index('\nfunc (c *DockerCluster) PauseNode',begin)
assert original[begin:end].count('go func() {')==1
assert (r/'sequential-observer.go').read_text()==original[:begin]+original[begin:end].replace('go func() {','func() {',1)+original[end:]
needle='\tcase result := <-killed:\n\t\treturn &result, nil\n';assert original.count(needle)==1
assert (r/'no-reply-wakeup.go').read_text()==original.replace(needle,'')
tests=['TestDockerKillObservationSeparatesExitFromCleanup','TestDockerKillObservationDoesNotWaitForKillReply','TestDockerKillObservationRejectsFailedListing','TestDockerKillObservationNative','TestDockerKillObservationNativeDelayedReply','TestDockerKillObservationPollWakesOnReplyWithoutTick','TestDockerKillObservationPollTicksAndCancellation']
for label in ['positive','sequential','no-reply-wakeup']:
 events=[json.loads(l) for l in (r/(label+'.jsonl')).read_text().splitlines()]
 assert not any(e['Action'] in ('build-fail','skip') for e in events)
 output=''.join(e.get('Output','') for e in events);assert 'panic: test timed out' not in output
 def actions(name):return [e['Action'] for e in events if e.get('Test')==name and e['Action'] in ('pass','fail')]
 package=[e['Action'] for e in events if not e.get('Test') and e['Action'] in ('pass','fail')]
 if label=='positive':
  assert package==['pass']
  for test in tests:assert actions(test)==['pass']
  for suffix in ('auto-remove-false','auto-remove-true'):assert actions('TestDockerKillObservationNative/'+suffix)==['pass']
 else:
  test=tests[1] if label=='sequential' else tests[5]
  assert package==['fail'] and actions(test)==['fail']
  marker='kill reply prevented observing stopped server' if label=='sequential' else 'kill reply did not wake state observer'
  assert marker in output and 'context deadline exceeded' in output
report=dict(run_id=37112424062,revision=revision,accepted=True,source_files_verified=len(source['files']),actual_race_tests=7,
 actual_docker_removal_modes=2,actual_delayed_reply=True,precise_compiled_controls=2,
 source_attribution_scope='nine inventoried files and exact hosted checkout',qualifies_ahead200=False,qualifies_full_release=False)
a.output.write_text(json.dumps(report,indent=2)+'\n');print(json.dumps(report))
